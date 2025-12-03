#!/usr/bin/env node

import { createInterface } from "node:readline";
import path from "node:path";
import { pathToFileURL } from "node:url";
import process from "node:process";
import fs from "node:fs";

type AdapterCallback = (payload: Record<string, unknown>) => unknown | Promise<unknown>;

interface AdapterOptions {
  modulePath: string;
  functionName: string;
}

interface SupervisorRequest {
  id?: string;
  type?: string;
  payload?: Record<string, unknown>;
}

interface SupervisorResponse {
  id: string;
  status: "ok" | "error";
  result?: unknown;
  error?: string;
}

function parseArgs(argv: string[]): AdapterOptions {
  const args = new Map<string, string>();
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith("--")) {
      continue;
    }
    const value = argv[i + 1];
    if (value && !value.startsWith("--")) {
      args.set(key.slice(2), value);
      i += 1;
    } else {
      args.set(key.slice(2), "");
    }
  }

  const modulePath = args.get("module");
  const functionName = args.get("function");

  if (!modulePath || !functionName) {
    process.stderr.write(
      "Usage: node dist/index.js --module <path/to/module.js> --function <exportName>\n"
    );
    process.exit(1);
  }

  const resolvedModulePath = path.resolve(modulePath);
  if (!fs.existsSync(resolvedModulePath)) {
    process.stderr.write(`Module file does not exist: ${resolvedModulePath}\n`);
    process.exit(1);
  }

  return {
    modulePath: resolvedModulePath,
    functionName,
  };
}

async function loadCallback(options: AdapterOptions): Promise<AdapterCallback> {
  const moduleUrl = pathToFileURL(options.modulePath).href;
  let imported: Record<string, unknown>;
  try {
    imported = (await import(moduleUrl)) as Record<string, unknown>;
  } catch (error) {
    throw new Error(`Failed to import module ${options.modulePath}: ${(error as Error).message}`);
  }

  const candidates = [
    imported[options.functionName],
    (imported.default as Record<string, unknown> | undefined)?.[options.functionName],
    options.functionName === "default" ? imported.default : undefined,
  ];

  for (const candidate of candidates) {
    if (typeof candidate === "function") {
      return candidate as AdapterCallback;
    }
  }

  throw new Error(
    `Function '${options.functionName}' not found in module ${options.modulePath}.` +
      " Ensure it is exported correctly."
  );
}

async function handleRequest(
  callback: AdapterCallback,
  request: SupervisorRequest
): Promise<SupervisorResponse> {
  const requestId = request.id ?? "unknown";

  try {
    if (!request.payload || typeof request.payload !== "object") {
      throw new Error("Payload missing or invalid.");
    }

    const result = await callback(request.payload);
    return {
      id: requestId,
      status: "ok",
      result: result ?? {},
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      id: requestId,
      status: "error",
      error: message,
    };
  }
}

function sendResponse(response: SupervisorResponse): void {
  try {
    process.stdout.write(`${JSON.stringify(response)}\n`);
  } catch (error) {
    process.stderr.write(`Failed to serialize response: ${(error as Error).message}\n`);
  }
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv);
  const callback = await loadCallback(options);

  const queue: string[] = [];
  let processing = false;

  const rl = createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
  });

  rl.on("line", (line: string) => {
    if (line.trim().length === 0) {
      return;
    }
    queue.push(line);
    if (!processing) {
      void processQueue();
    }
  });

  rl.on("close", () => {
    process.stderr.write("stdin closed; shutting down adapter.\n");
    process.exit(0);
  });

  async function processQueue(): Promise<void> {
    processing = true;
    while (queue.length > 0) {
      const line = queue.shift();
      if (!line) {
        continue;
      }
      let request: SupervisorRequest;
      try {
        request = JSON.parse(line) as SupervisorRequest;
      } catch (error) {
        process.stderr.write(`Failed to parse JSON from supervisor: ${(error as Error).message}\n`);
        sendResponse({
          id: "unknown",
          status: "error",
          error: "Invalid JSON payload.",
        });
        continue;
      }

      const response = await handleRequest(callback, request);
      sendResponse(response);
    }
    processing = false;
  }

  const shutdown = () => {
    process.stderr.write("Received shutdown signal. Exiting.\n");
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

void main().catch((error) => {
  process.stderr.write(`Fatal adapter error: ${(error as Error).stack ?? error}\n`);
  process.exit(1);
});

