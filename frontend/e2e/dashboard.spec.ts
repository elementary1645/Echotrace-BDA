import { test, expect } from '@playwright/test';

test('real API overview, filters, benchmark and original monitor', async ({page})=>{
  const errors:string[]=[];page.on('pageerror',e=>errors.push(e.message));
  await page.goto('/');
  await expect(page.getByRole('heading',{name:'Overview',exact:true})).toBeVisible();
  await expect(page.getByText('100,000',{exact:true})).toBeVisible();
  await expect(page.getByText('SIMULATED · synthetic',{exact:true})).toBeVisible();
  await page.screenshot({path:'../bda/reports/phase5/overview-desktop.png',fullPage:true});
  await page.getByRole('link',{name:'Historical Analytics'}).click();
  await expect(page.getByText('100,000 events in selection')).toBeVisible();
  await page.getByLabel('Until (exclusive, UTC)').fill('2026-01-03');
  await expect(page.getByText('28,426 events in selection')).toBeVisible();
  await page.getByLabel('Category',{exact:true}).selectOption('ALARM');
  await expect(page.getByText('1,493 events in selection')).toBeVisible();
  await page.getByRole('link',{name:'Anomaly Intelligence'}).click();
  await expect(page.getByRole('heading',{name:'Detected anomaly timeline'})).toBeVisible();
  await page.getByRole('link',{name:'Performance Lab'}).click();
  await expect(page.getByRole('heading',{name:'Median execution time'})).toBeVisible();
  await expect(page.getByText('70.69',{exact:true}).first()).toBeVisible();
  await page.getByRole('link',{name:'System Health'}).click();
  await expect(page.getByRole('heading',{name:'Pipeline observability'})).toBeVisible();
  await page.getByRole('link',{name:'Live Analytics'}).click();
  await expect(page.getByText(/WebSocket open/)).toBeVisible();
  await page.getByRole('link',{name:'Acoustic Monitor'}).click();
  await expect(page.getByText('SIMULATED',{exact:true}).first()).toBeVisible();
  expect(errors).toEqual([]);
});

test('mobile navigation and no page overflow',async({page})=>{
  await page.setViewportSize({width:390,height:844});
  await page.goto('/');
  await expect(page.getByText('100,000',{exact:true})).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  await page.screenshot({path:'../bda/reports/phase5/overview-mobile.png',fullPage:true});
  await page.getByRole('link',{name:'Historical Analytics'}).click();
  await expect(page.getByLabel('From (UTC)')).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
});

test('HTTP failure has a retry state',async({page})=>{
  await page.route('**/api/v1/analytics/overview',route=>route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({error:'Historical export unavailable'})}));
  await page.goto('/');
  await expect(page.getByRole('alert')).toContainText('Historical export unavailable');
  await expect(page.getByRole('button',{name:'Retry'})).toBeVisible();
});

test('API connectivity loss and restoration recovers on Retry',async({page,context})=>{
  await page.goto('/#5');
  await expect(page.getByRole('heading',{name:'Pipeline observability'})).toBeVisible();
  await context.setOffline(true);
  await page.getByRole('button',{name:/Refresh/}).click();
  await expect(page.getByRole('alert')).toBeVisible();
  await context.setOffline(false);
  await page.getByRole('button',{name:'Retry'}).click();
  await expect(page.getByRole('heading',{name:'Pipeline observability'})).toBeVisible();
  await expect(page.getByRole('alert')).toHaveCount(0);
});

test('workspace navigation and filters support keyboard activation',async({page})=>{
  await page.goto('/');
  await page.getByRole('link',{name:/ECHOTRACE/}).focus();
  await page.keyboard.press('Tab');
  await expect(page.getByRole('link',{name:/Overview/})).toBeFocused();
  await page.getByRole('link',{name:'Historical Analytics'}).focus();
  await page.keyboard.press('Enter');
  await expect(page.getByLabel('From (UTC)')).toBeVisible();
  await expect(page.getByLabel('Category',{exact:true})).toContainText('ALARM');
  await page.getByLabel('Category',{exact:true}).focus();
  await page.keyboard.press('ArrowDown');
  await expect(page.getByLabel('Category',{exact:true})).toHaveValue('ALARM');
  await expect(page.getByText(/5,987 events in selection/)).toBeVisible();
  await page.keyboard.press('Tab');
  await expect(page.getByRole('button',{name:'Switch to daily'})).toBeFocused();
});
