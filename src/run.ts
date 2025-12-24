import { Service } from "./service.js";

const service = new Service();
await service.initialize();
await service.serve();
