import { readFileSync } from "node:fs";

function extractExports(file) {
  const src = readFileSync(file, "utf8");
  const symbols = new Set();
  // Match export { ... } and export type { ... } blocks (multi-line).
  const blockRe = /export\s+(?:type\s+)?\{([^}]*)\}\s*from/g;
  let m;
  while ((m = blockRe.exec(src)) !== null) {
    const inner = m[1];
    // split on commas, but ignore commas inside <...> generics (none expected in export lists)
    for (let part of inner.split(",")) {
      part = part.trim();
      if (!part) continue;
      // strip "type " prefix and "as X" alias -> keep the exported name (after as, else before)
      let s = part.replace(/^type\s+/, "");
      const asMatch = s.match(/^(.*?)\s+as\s+(.+)$/);
      if (asMatch) s = asMatch[2].trim();
      else s = s.trim();
      if (s) symbols.add(s);
    }
  }
  // Also single-line: export const/function/class X
  for (const re of [/^export\s+(?:const|let|var|function|class|enum)\s+([A-Za-z0-9_$]+)/gm]) {
    let mm;
    while ((mm = re.exec(src)) !== null) symbols.add(mm[1]);
  }
  return [...symbols].sort();
}

for (const file of process.argv.slice(2)) {
  const syms = extractExports(file);
  console.log(`# ${file}: ${syms.length} exported symbols`);
  for (const s of syms) console.log(s);
}
