import {test,expect} from '@playwright/test';

test('isolated pipeline output is served and presented with exact provenance',async({page,request})=>{
  test.skip(!process.env.P5_BROWSER_URL,'requires the documented generated-output server');
  const url=process.env.P5_BROWSER_URL!;
  const errors:string[]=[];page.on('pageerror',e=>errors.push(e.message));
  const response=await request.get(url+'/api/v1/analytics/streaming');expect(response.status()).toBe(200);
  const body=await response.json();
  expect(body.provenance.origin).toBe('streaming-finalized');expect(body.provenance.event_count).toBe(25);
  const acoustic=body.rows.filter((r:any)=>r.source_id==='p5-acoustic');
  expect(acoustic.reduce((sum:number,r:any)=>sum+r.event_count,0)).toBe(8);
  expect(acoustic.every((r:any)=>r.source==='SIMULATED')).toBe(true);
  let apiReceivedAt:number|null=null;
  page.on('response',r=>{if(r.url().endsWith('/api/v1/analytics/streaming'))apiReceivedAt=performance.now();});
  const before=performance.now();await page.goto(url+'/#1');
  await expect(page.getByRole('heading',{name:'Spark streaming aggregates'})).toBeVisible();
  await expect(page.getByRole('cell',{name:'p5-acoustic',exact:true}).first()).toBeVisible();
  await expect(page.getByText('MIXED; inspect row source · streaming-finalized')).toBeVisible();
  const visibleAt=performance.now();
  console.log(JSON.stringify({boundary:'API response received to exact row visible; one browser observation',milliseconds:apiReceivedAt===null?null:visibleAt-apiReceivedAt,population:1,navigation_to_visible_ms:visibleAt-before}));
  await page.screenshot({path:'../bda/reports/phase5/pipeline-dashboard.png',fullPage:true});
  await page.getByRole('link',{name:'System Health'}).click();
  await expect(page.getByRole('cell',{name:'classified',exact:true})).toBeVisible();
  await expect(page.getByRole('cell',{name:'stopped',exact:true}).first()).toBeVisible();
  await page.screenshot({path:'../bda/reports/phase5/system-health.png',fullPage:true});
  expect(errors).toEqual([]);
});
