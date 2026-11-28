const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const code=fs.readFileSync('static/queue.js','utf8');
const fn=code.match(/  function _queueReadableError\(value\) \{[\s\S]*?\n  \}/)[0];
const ctx={};vm.createContext(ctx);vm.runInContext(fn,ctx);
assert.equal(ctx._queueReadableError('JAVStash: HTTP 403: <!DOCTYPE html><title>Cloudflare</title>'),'JAVStash: HTTP 403: blocked by Cloudflare; retry later.');
assert.equal(ctx._queueReadableError('Disk full'),'Disk full');
assert.equal(ctx._queueReadableError('HTTP 502: <html>bad gateway</html>'),'HTTP 502: source returned an error page; retry later.');
console.log('PASS: stored HTML errors become readable messages');
