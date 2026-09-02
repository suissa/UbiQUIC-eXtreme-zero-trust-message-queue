import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import YAML from "yaml";

const root = path.resolve(import.meta.dirname, "..");
const schemaPath = path.join(root, "schemas", "universal-adapter.schema.yml");
const source = fs.readFileSync(schemaPath, "utf8");
const schema = YAML.parse(source);
const hash = crypto.createHash("sha256").update(source).digest("hex");

const technologies = schema.$defs.adapter.properties.technology.enum;
const modes = schema.$defs.semantics.properties.mode.enum;
const securityProfiles = schema.$defs.securityProfile.properties.profile.enum;
const keyEstablishments = schema.$defs.securityProfile.properties.key_establishment.enum;

function ensure(dir) { fs.mkdirSync(dir, { recursive: true }); }
function quoted(values) { return values.map(v => JSON.stringify(v)).join(" | "); }
function zigTag(v) { return v.replaceAll("-", "_"); }
function rustTag(v) { return v.split(/[-_]/).map(x => x[0].toUpperCase() + x.slice(1)).join(""); }
function goTag(v) { return rustTag(v); }

const header = `GENERATED from schemas/universal-adapter.schema.yml\nSchema SHA-256: ${hash}`;

const outputs = {
  "generated/typescript/schema-types.ts": `// ${header.replaceAll("\n", "\n// ")}\nexport type AdapterTechnology = ${quoted(technologies)};\nexport type AdapterMode = ${quoted(modes)};\nexport type SecurityProfile = ${quoted(securityProfiles)};\nexport type KeyEstablishment = ${quoted(keyEstablishments)};\nexport const ADAPTER_SCHEMA_SHA256 = ${JSON.stringify(hash)} as const;\n`,
  "generated/zig/schema_types.zig": `// ${header.replaceAll("\n", "\n// ")}\npub const AdapterTechnology = enum { ${technologies.map(zigTag).join(", ")} };\npub const AdapterMode = enum { ${modes.map(zigTag).join(", ")} };\npub const SecurityProfile = enum { local_trusted_v1, mtls_dpop_v1, xzt_v1 };\npub const SchemaSha256 = "${hash}";\n`,
  "generated/rust/schema_types.rs": `// ${header.replaceAll("\n", "\n// ")}\n#[derive(Debug, Clone, Copy, PartialEq, Eq)]\npub enum AdapterTechnology { ${technologies.map(rustTag).join(", ")} }\n#[derive(Debug, Clone, Copy, PartialEq, Eq)]\npub enum AdapterMode { ${modes.map(rustTag).join(", ")} }\npub const ADAPTER_SCHEMA_SHA256: &str = "${hash}";\n`,
  "generated/go/schema_types.go": `// ${header.replaceAll("\n", "\n// ")}\npackage universaladapter\n\ntype AdapterTechnology string\nconst (\n${technologies.map(v => `\tTechnology${goTag(v)} AdapterTechnology = ${JSON.stringify(v)}`).join("\n")}\n)\n\ntype AdapterMode string\nconst (\n${modes.map(v => `\tMode${goTag(v)} AdapterMode = ${JSON.stringify(v)}`).join("\n")}\n)\n\nconst AdapterSchemaSHA256 = "${hash}"\n`
};

for (const [rel, content] of Object.entries(outputs)) {
  const target = path.join(root, rel);
  ensure(path.dirname(target));
  fs.writeFileSync(target, content);
  console.log(`generated ${rel}`);
}

console.log(`schema ${schema.$id}`);
console.log(`sha256 ${hash}`);
console.log(`technologies ${technologies.join(", ")}`);
