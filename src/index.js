const assert = require("assert");
const { format, addDays } = require("date-fns");
const bigqueryDataTransfer = require("@google-cloud/bigquery-data-transfer");
const client = new bigqueryDataTransfer.v1.DataTransferServiceClient();

const [, , _name = "", _start = "", _end = "", _timeZone = "", _sequential = "1"] = process.argv;
const name = _name;
const startDate = _start;
const endDate = _end;
const timeZone = _timeZone.toUpperCase() || "UTC";
const sequential = _sequential === "1";
const location = "us-central1";
assert(/^.+$/.test(name), `"scheduledQueryName" is required`);
assert(/^\d{4}-\d{2}-\d{2}$/.test(startDate), `"startDate" must be formatted as yyyy-MM-dd`);
assert(/^\d{4}-\d{2}-\d{2}$/.test(endDate), `"endDate" must be formatted as yyyy-MM-dd`);
assert(/^UTC([+-]\d+)?$/.test(timeZone), `"timeZone" must be formatted as "UTC" or "UTC+{hour}" or "UTC-{hour}"`);
assert(/^[01]?$/.test(_sequential), `"sequential" must be 0 or 1 or undefined`);

async function main() {
  const projectId = await client.getProjectId();
  console.log({ projectId, scheduledQueryName: name, startDate, endDate, timeZone, sequential });
  console.log("waiting 30 sec... (press CTRL + C for cancel operation)");
  await new Promise((resolve) => setTimeout(resolve, 1000 * 30));

  const transferConfigs = await client.listTransferConfigs({ parent: client.projectPath(projectId, location) });
  const scheduledQueryConfigs = transferConfigs
    .map((c) => (c && c[0] ? c[0] : null))
    .filter((c) => c && c.dataSourceId === "scheduled_query" && c.displayName === name);

  const [scheduledQueryConfig] = scheduledQueryConfigs;
  if (!scheduledQueryConfig) {
    console.log(`Scheduled Query config is not found: "${name}"`);
    return;
  }

  const runScheduledQuery = async (_startDate, _endDate) => {
    const start = Date.now();
    const startTime = new Date(`${_startDate} 00:00:00 ${timeZone}`);
    const endTime = new Date(`${_endDate} 00:00:00 ${timeZone}`);
    console.log(`Start Scheduled Query in ${sequential ? "sequential" : "parallel"}`);
    console.log({ name: scheduledQueryConfig.displayName, startDate: _startDate, endDate: _endDate, timeZone });
    const [{ runs }] = await client.startManualTransferRuns({
      parent: scheduledQueryConfig.name,
      requestedTimeRange: {
        startTime: {
          seconds: startTime.valueOf() / 1000,
          nanos: 0,
        },
        endTime: {
          seconds: endTime.valueOf() / 1000,
          nanos: 0,
        },
      },
    });

    if (sequential) {
      await new Promise((resolve) => setTimeout(resolve, 1000 * 60));
    } else {
      console.log("SCHEDULED");
      return;
    }

    let result;
    while (true) {
      const [_result] = await client.getTransferRun({ name: runs[0].name });
      result = _result;
      if (result.state === "SUCCEEDED") {
        break;
      } else if (result.state === "FAILED" || result.state === "CANCELLED") {
        console.error(result);
        throw new Error(`State: ${result.state}`);
      }
      console.log(`Processing: ${(Date.now() - start) / 1000} sec.`);
      await new Promise((resolve) => setTimeout(resolve, 1000 * 10));
    }
    console.log(result.state);
  };

  if (sequential) {
    let targetDate = startDate;
    while (new Date(targetDate) < new Date(endDate)) {
      const nextDate = format(addDays(new Date(targetDate), 1), "yyyy-MM-dd");
      await runScheduledQuery(targetDate, nextDate);
      targetDate = nextDate;
    }
  } else {
    await runScheduledQuery(startDate, endDate);
  }
}

main().catch(console.error);
