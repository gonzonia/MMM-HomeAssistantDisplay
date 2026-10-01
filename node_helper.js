/* Magic Mirror
 * Node Helper: MMM-HomeAssistantDisplay
 *
 * By Brian Towles
 * MIT Licensed.
 */
var backoff = require('backoff')
const http = require("http");
const https = require("https");
const NodeHelper = require("node_helper");
const HomeAssistantWS = require("homeassistant-ws");
const Logger = require("./helpers/Logger");
util = require('util'),


module.exports = NodeHelper.create({
	start,
	stop,
	socketNotificationReceived,
	connect,
	reconnectWebsocket,
	connectWebsocket,
	buildHttpUrl,
	onStateChangedEvent,
	evaluateTemplate,
	onWebsocketCloseEvent,
	backoffWSConnection,
});

function start() {
	this.logger = new Logger(this.name);
	if (config.debuglogging) {
		this.logger.debug("MMM-HomeAssistantDisplay helper started...");
	}
	this.connections = {};
}

function stop() {
	for (const connection in this.connections) {
		this.connections[connection].websocket.unsubscribeFromEvent("state_changed");
	}
}

function socketNotificationReceived(notification, payload) {
	if (config.debuglogging) {
		this.logger.debug(`Recieved notification ${notification}`, payload);
	}
	if (notification !== "CONNECT" && (!payload.identifier || !this.connections[payload.identifier])) {
		this.logger.error(`No connection for ${payload.identifier} found`);
		return;
	}
	switch (notification) {
		case "CONNECT":
			this.connect(payload);
			break;
		case "RECONNECT_WS":
			this.reconnectWebsocket(payload);
			break;
		case "SET_WATCHED_ENTITY":
			if (!this.connections[payload.identifier].entities.includes(payload.entity)) {
				if (config.debuglogging) {
					this.logger.debug(`Registering entity ${payload.entity}`);
				}
				this.connections[payload.identifier].entities.push(payload.entity);
			}
			break;
		case "RENDER_MODULE_DISPLAY_TEMPLATE":
			this.evaluateTemplate(payload).then((ret) => {
				this.sendSocketNotification("MODULE_DISPLAY_RENDERED", ret);
			}).catch((err) => {
				this.logger.error(
					"Unable to evaluate template",
					err
				);
			});
			break;
		case "RENDER_SECTION_DISPLAY_TEMPLATE":
			this.evaluateTemplate(payload).then((ret) => {
				this.sendSocketNotification("SECTION_DISPLAY_RENDERED", {
					...ret, 
					section: payload.section
				});
			}).catch((err) => {
				this.logger.error(
					"unable to evaluate section template",
					err
				);
			});
			break;			
	}
}

/**
 * Render a template through Home Assistant's REST API (POST /api/template).
 * Resolves with the rendered text as a string; rejects on network errors,
 * timeouts and non-2xx responses.
 */
function renderTemplate(connectionConfig, template) {
	return new Promise((resolve, reject) => {
		const body = JSON.stringify({ template });
		const transport = connectionConfig.useTLS ? https : http;
		const options = {
			host: connectionConfig.host,
			port: connectionConfig.port || 8123,
			path: "/api/template",
			method: "POST",
			headers: {
				"Authorization": `Bearer ${connectionConfig.token}`,
				"Content-Type": "application/json",
				"Content-Length": Buffer.byteLength(body),
			},
		};
		if (connectionConfig.useTLS) {
			options.rejectUnauthorized = !connectionConfig.ignoreCert;
		}
		const req = transport.request(options, (res) => {
			let data = "";
			res.setEncoding("utf8");
			res.on("data", (chunk) => {
				data += chunk;
			});
			res.on("end", () => {
				if (res.statusCode < 200 || res.statusCode >= 300) {
					reject(new Error(`Home Assistant returned HTTP ${res.statusCode}: ${data}`));
					return;
				}
				resolve(data);
			});
		});
		req.setTimeout(10000, () => {
			req.destroy(new Error("Template request timed out"));
		});
		req.on("error", reject);
		req.end(body);
	});
}

async function evaluateTemplate(payload) {
	if (config.debuglogging) {
		this.logger.debug(`Evaluating template for ${payload.template}`);
	}
	const { connectionConfig } = this.connections[payload.identifier];
	const response = await renderTemplate(connectionConfig, payload.template);
	return {
		identifier: payload.identifier,
		render: response
	}
}

function buildHttpUrl(config) {
	if (config.useTLS){
		schema = "https"
	}
	else {
		schema = "http"
	}
	var url = `${schema}://${config.host}`;
	return url;
}


async function connect(payload) {
	const connectionConfig = {
		host: payload.host,
		port: payload.port,
		token: payload.token,
		ignoreCert: payload.ignoreCert,
		useTLS: payload.useTLS,
	};
	this.logger.info(`HomeAssistant connected for ${payload.identifier}`);
	this.connections[payload.identifier] = {
		connectionConfig,
		entities: []
	};

	await this.backoffWSConnection(payload.identifier, connectionConfig)
}

async function backoffWSConnection(identifier, connectionConfig) {
	self = this;
	var call = backoff.call(this.connectWebsocket, this, identifier, connectionConfig, function(err, res) {		
		if (err) {
			self.logger.info(`Unable to connect to Home Assistant for ${identifier}: ` + err.message);
		} else {
			self.logger.info(`Conected to Home Assistant for ${identifier} after ${call.getNumRetries()} retries`);	
		}
	});

	call.retryIf(function(err) {
		 return true; 
	});

	call.setStrategy(new backoff.ExponentialStrategy({
		initialDelay: 10,
		maxDelay: 10000
	}));

	call.start();
}

function connectWebsocket(obj, identifier, connectionConfig, callback) {
	var self = obj;
	HomeAssistantWS.default({
		...connectionConfig,
		protocol: ((connectionConfig.useTLS) ? "wss" : "ws")
	})
		.then((hassWs) => {
			self.connections[identifier].websocket = hassWs;
			hassWs.on("state_changed", onStateChangedEvent.bind(self));
			hassWs.on("ws_close", onWebsocketCloseEvent.bind(self));
			callback(null, hassWs);
		})
		.catch((err) => {
			self.logger.error(
				`Unable to connect to Home Assistant for module ${identifier} failed with message: `,
				err.message
			);
			callback(err, null);
		});
	return;
}

async function reconnectWebsocket(payload) {
	const connectionConfig = {
		host: payload.host,
		port: payload.port,
		token: payload.token,
		useTLS: payload.useTLS,
		ignoreCert: payload.ignoreCert
	};
	for (const connection in this.connections) {
		if (connection == payload.identifier){
			this.logger.info(`Reconnecting to Home Assistant websocket for ${payload.identifier}`);		
			await this.backoffWSConnection(payload.identifier, connectionConfig)
		}		
	}
}

function onStateChangedEvent(event) {
	if (config.debuglogging) {
		this.logger.debug(`Got state change for ${event.data.entity_id}`);
	}
	for (const connection in this.connections) {
		if (this.connections[connection].entities.includes(event.data.entity_id)) {
			this.logger.debug(`Found listening connection (${connection}) for entity ${event.data.entity_id}`);
			this.sendSocketNotification("CHANGED_STATE", {
				identifier: connection,
				cause: event.data.entity_id,
			});
		}
	}
}

function onWebsocketCloseEvent(event) {
	for (const connection in this.connections) {
		if (event.target == this.connections[connection].websocket.rawClient.ws) {
			this.logger.debug(`Hass WS Disconnected (${connection})`);
			this.sendSocketNotification("HASSWS_DISCONNECTED", {
				identifier: connection,
			});
		}
	}
}
