import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const packageJsonPath = path.join(repoRoot, "package.json");
const skillsRoot = path.join(repoRoot, "skills");

interface PackageManifest {
  version: string;
  files?: string[];
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
  pi?: {
    extensions?: string[];
    skills?: string[];
  };
}

describe("Pi package resources", () => {
  it("publishes the extension and skill directory", async () => {
    const manifest = JSON.parse(await fs.readFile(packageJsonPath, "utf8")) as PackageManifest;

    expect(manifest.files).toContain("skills");
    expect(manifest.files).toContain("plugins/herdr");
    expect(manifest.files).toContain("herdr-plugin.toml");
    expect(manifest.pi?.extensions).toEqual(["./src/extension/index.ts"]);
    expect(manifest.pi?.skills).toEqual(["./skills"]);

    const extensionPath = manifest.pi?.extensions?.[0];
    const skillPath = manifest.pi?.skills?.[0];
    if (extensionPath === undefined || skillPath === undefined) {
      throw new Error("Pi package resources are missing from package.json.");
    }
    await expect(fs.stat(path.join(repoRoot, extensionPath))).resolves.toBeDefined();
    await expect(fs.stat(path.join(repoRoot, skillPath))).resolves.toBeDefined();
  });

  it("tests the latest Pi SDK with open-ended peer support", async () => {
    const manifest = JSON.parse(await fs.readFile(packageJsonPath, "utf8")) as PackageManifest;
    for (const packageName of [
      "@earendil-works/pi-ai",
      "@earendil-works/pi-coding-agent",
      "@earendil-works/pi-tui",
    ]) {
      expect(manifest.devDependencies?.[packageName]).toBe("0.85.0");
      expect(manifest.peerDependencies?.[packageName]).toBe(">=0.84.2");
    }
    expect(manifest.dependencies?.["@earendil-works/pi-server"]).toBe(">=0.85.0");
  });

  it("ships one matching Herdr plugin from the package root", async () => {
    const manifest = JSON.parse(await fs.readFile(packageJsonPath, "utf8")) as PackageManifest;
    const herdrManifest = await fs.readFile(path.join(repoRoot, "herdr-plugin.toml"), "utf8");

    expect(herdrManifest).toContain('id = "osolmaz.pi-workflows"');
    expect(herdrManifest).toContain(`version = "${manifest.version}"`);
    expect(herdrManifest).toContain('command = ["node", "plugins/herdr/viewer.mjs"]');
    await expect(fs.stat(path.join(repoRoot, "plugins/herdr/viewer.mjs"))).resolves.toBeDefined();
  });

  it("keeps local references in the workflow skill inside the package", async () => {
    const skillPath = path.join(skillsRoot, "pi-workflows", "SKILL.md");
    const markdown = await fs.readFile(skillPath, "utf8");
    const links = [...markdown.matchAll(/\[[^\]]+\]\((\.\.\/\.\.\/[^)]+)\)/gu)]
      .map((match) => match[1])
      .filter((link): link is string => link !== undefined);

    expect(links.length).toBeGreaterThan(0);
    for (const link of links) {
      await expect(
        fs.stat(path.resolve(path.dirname(skillPath), link.split("#")[0]!)),
      ).resolves.toBeDefined();
    }
  });
});
