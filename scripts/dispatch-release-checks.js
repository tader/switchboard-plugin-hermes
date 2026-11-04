import { execFileSync } from "node:child_process";
const gh = (args) => execFileSync("gh", args, { encoding: "utf8" });
const prs = JSON.parse(process.env.RELEASE_PRS ?? "[]");
for (const proposed of prs) {
  const pr = JSON.parse(
    gh(["api", "repos/" + process.env.GH_REPO + "/pulls/" + proposed.number]),
  );
  if (
    pr.base.ref !== "main" ||
    pr.head.repo.full_name !== process.env.GH_REPO ||
    !pr.head.ref.startsWith("release-please--branches--main") ||
    pr.state !== "open" ||
    !/^[a-f0-9]{40}$/.test(pr.head.sha)
  )
    throw Error("Unexpected release PR; refusing dispatch");
  gh([
    "workflow",
    "run",
    "check.yml",
    "--ref",
    pr.head.ref,
    "-f",
    "expected_sha=" + pr.head.sha,
  ]);
}
