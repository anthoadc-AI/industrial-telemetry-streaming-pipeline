'use strict';

// Syntax check for every JavaScript file tracked by git: `node --check` on
// each, the JS half of `make lint` (owner decision, Task 2.6). No ESLint
// configuration to maintain; it catches parse errors, not style.
//
//   node scripts/check_js_syntax.js
//
// Exit status: 0 when every file parses, 1 otherwise.

const { spawnSync } = require('node:child_process');

const listed = spawnSync('git', ['ls-files', '-z', '*.js'], { encoding: 'utf8' });
if (listed.status !== 0) {
  process.stderr.write(`git ls-files failed: ${listed.stderr}\n`);
  process.exit(1);
}
const files = listed.stdout.split('\0').filter(Boolean);
let failed = 0;
for (const file of files) {
  const check = spawnSync(process.execPath, ['--check', file], { encoding: 'utf8' });
  if (check.status !== 0) {
    failed += 1;
    process.stdout.write(`FAIL ${file}\n${check.stderr}`);
  }
}
process.stdout.write(`check-js-syntax: ${files.length} file(s) checked, ${failed} failed\n`);
process.exit(failed ? 1 : 0);
