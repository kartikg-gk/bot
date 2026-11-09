import express from "express";

import { gatewayProxy } from "./proxy.js";

const app = express();
app.disable("x-powered-by");
app.use(gatewayProxy());

export default app;
