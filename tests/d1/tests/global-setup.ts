import { execFileSync } from "node:child_process";
import { writeSync } from "node:fs";
import { fileURLToPath } from "node:url";

const fixtureDirectory = fileURLToPath(new URL("..", import.meta.url));
const expectedWorkerBuildVersion = "0.8.5";

function writeSetupMessage(message: string): void {
	writeSync(process.stdout.fd, `[setup] ${message}\n`);
}

export default function globalSetup(): void {
	writeSetupMessage("Checking `worker-build` version");

	const workerBuildVersion = execFileSync("worker-build", ["--version"], {
		encoding: "utf8",
	}).trim();

	if (workerBuildVersion !== expectedWorkerBuildVersion) {
		throw new Error(
			`expected worker-build ${expectedWorkerBuildVersion}, got ${workerBuildVersion}`,
		);
	}

	writeSetupMessage("Building the D1 integration-test worker");

	execFileSync("worker-build", ["--release", "--no-panic-recovery"], {
		cwd: fixtureDirectory,
		stdio: "inherit",
	});
}
