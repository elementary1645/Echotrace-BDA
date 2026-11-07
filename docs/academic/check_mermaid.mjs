/** Optional Mermaid syntax check; does not add or install application dependencies.
 * MERMAID_MODULE=/absolute/path/to/mermaid/dist/mermaid.esm.mjs node docs/academic/check_mermaid.mjs
 * Uses the existing frontend jsdom dev dependency to initialize the parser DOM.
 */
import {existsSync,readFileSync,readdirSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
const root=fileURLToPath(new URL('../../',import.meta.url));
const modulePath=process.env.MERMAID_MODULE??join(root,'frontend/node_modules/mermaid/dist/mermaid.esm.mjs');
const domPath=join(root,'frontend/node_modules/jsdom/lib/api.js');
if(!existsSync(modulePath)||!existsSync(domPath)){
  console.error('Optional parser unavailable. Set MERMAID_MODULE to a separately prepared Mermaid ESM module and install frontend dev dependencies.');
  process.exit(2);
}
const {JSDOM}=await import(pathToFileURL(resolve(domPath)).href);
const dom=new JSDOM('<!doctype html><html><body></body></html>');
globalThis.window=dom.window;globalThis.document=dom.window.document;
const {default:mermaid}=await import(pathToFileURL(resolve(modulePath)).href);
mermaid.initialize({startOnLoad:false});
function markdownFiles(dir){
  return readdirSync(dir,{withFileTypes:true}).flatMap(entry=>entry.isDirectory()?markdownFiles(join(dir,entry.name)):entry.name.endsWith('.md')?[join(dir,entry.name)]:[]);
}
const files=[join(root,'README.md'),...markdownFiles(join(root,'bda')),...markdownFiles(join(root,'docs'))];
const checks=[];
for(const file of files){
  const blocks=[...readFileSync(file,'utf8').matchAll(/^```mermaid\n([\s\S]*?)^```\s*$/gm)];
  for(const [index,block] of blocks.entries()){
    await mermaid.parse(block[1]);checks.push({file:file.slice(root.length),block:index+1,status:'passed'});
  }
}
console.log(JSON.stringify({mermaid_syntax_checks:checks.length,checks},null,2));
dom.window.close();
