const topic = Deno.args[0];
const connector = Deno.args[1];
const consumergroup = Deno.args[2];
const pauseThresholdLag = parseInt(Deno.args[3], 10);
const resumeThresholdLag =  parseInt(Deno.args[4], 10);
const prometheusHost = Deno.args[5];
const kafkaConnectHost = Deno.args[6];

console.log(`Running with config: `);
console.log({ topic, connector, consumergroup, pauseThresholdLag, resumeThresholdLag, prometheusHost, kafkaConnectHost });

const query = `sum(kafka_consumergroup_lag{topic="${topic}",consumergroup="${consumergroup}"}) by (consumergroup) >= 0`
const intervalMs = 10000;

let numCaught = 0;
async function main() {
				setInterval(async () => {
								try {
												await checkLag();
												numCaught = 0;
								} catch (e) {
												console.error('something threw when checking lag!', e);
												numCaught += 1;
												// maybe we got booted off of vpn
												if (numCaught >= 3) {
																console.log("too many catches, exiting!");
																Deno.exit(1);
												}
								}
				}, intervalMs);
}

async function checkLag() {
				console.log('checking lag metric...');
				const response = await fetch(`${prometheusHost}/api/v1/query?query=${query}`);
				// console.log({ response });
				if (response.ok) {
								const responseJson = await response.json();
								// console.log({ responseJson });
								const lag = responseJson.data.result[0].value[1];
								console.log(`snowflakesink lag on ${topic} topic is ${lag}`);
								if (lag >= pauseThresholdLag) {
												await pauseConnector();
								} else if (lag <= resumeThresholdLag) {
												await resumeConnector();
								} else {
												console.log('between thresholds, doing nothing.');
								}
				}
}

main();

async function pauseConnector() {
				const connectorStatus = await getConnectorStatus();
				if (connectorStatus === "PAUSED") {
								console.log('datoms connector already paused, no need to pause');
								return;
				}
				console.log('pausing datoms connector...');
				const response = await fetch(`${kafkaConnectHost}/connectors/${connector}/pause`, {
								method: 'PUT'
				});
				if (response.ok) {
								console.log('paused.');
				} else {
								console.log({ response });
				}
}

async function resumeConnector() {
				const connectorStatus = await getConnectorStatus();
				if (connectorStatus === "RUNNING") {
								console.log('datoms connector already running, no need to resume');
								return;
				}
				console.log('resuming datoms connector...');
				const response = await fetch(`${kafkaConnectHost}/connectors/${connector}/resume`, {
								method: 'PUT'
				});
				if (response.ok) {
								console.log('resumed.');
				} else {
								console.log({ response });
				}
}

async function getConnectorStatus() {
				console.log('checking datoms connector status...');
				const response = await fetch(`${kafkaConnectHost}/connectors/${connector}/status`);
				const responseJson = await response.json();
				const connectorStatus = responseJson.connector.state;
				console.log(`connector status is ${connectorStatus}`);
				return connectorStatus;
}
