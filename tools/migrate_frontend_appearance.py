#!/usr/bin/env python3
from pathlib import Path
p=Path('Index.html'); s=p.read_text(); L=chr(60); G=chr(62)
def sw(a,b,n):
 global s
 c=s.count(a)
 if c!=1: raise RuntimeError(f'{n}: {c}')
 s=s.replace(a,b,1)
sw(f'{L}title{G}Commodity Price Tracker{L}/title{G}',f'{L}title{G}Bitcoin Purchasing Power Dashboard{L}/title{G}','title')
sw(f'{L}div class="title" id="pageTitle"{G}Commodity Price Tracker{L}/div{G}',f'{L}div class="title" id="pageTitle"{G}Bitcoin Purchasing Power Dashboard{L}/div{G}','page title')
sw('Commodity Price Tracker','Bitcoin Purchasing Power Dashboard','remaining title')
sw('Include stale values','Include carried-forward values','stale label')
sw('Exclude stale values','Fresh observations only','fresh label')
sw('Basket cost is computed as the weighted average cost of the included basket items.','Basket cost is the sum of one standardized package of every included core grocery item; reference assets are excluded.','explanation')
sw('Basket cost in Dollars over time','Fixed basket total in dollars over time','dollar chart')
sw('Basket cost in sats over time','Fixed basket total in sats over time','sats chart')
sw('text: "Basket index sats"','text: "Fixed basket total (sats)"','axis')
css='''
    .mission-copy{margin-top:6px;color:var(--muted);font-size:12px}
    #ppHeroKpis{margin-top:14px;grid-template-columns:repeat(4,minmax(0,1fr))}
    @media (max-width:900px){#ppHeroKpis{grid-template-columns:repeat(2,minmax(0,1fr))}}
    @media (max-width:560px){#ppHeroKpis{grid-template-columns:1fr}}
    .kpi.hero-primary{border-color:rgba(247,147,26,.55);background:linear-gradient(145deg,rgba(247,147,26,.19),rgba(125,211,252,.08))}
    .kpi .delta{margin-top:6px;font-size:12px;font-weight:800}.kpi .detail{margin-top:5px;color:var(--muted);font-size:11px}
    .confidence-pill{display:inline-flex;min-width:34px;padding:4px 9px;border-radius:999px;font-weight:950;border:1px solid rgba(255,255,255,.2)}
    .confidence-a,.confidence-b{color:var(--good)}.confidence-c{color:var(--warn)}.confidence-d,.confidence-f{color:var(--bad)}
'''
end=L+'/style'+G
idx=s.rfind(end)
if idx<0: raise RuntimeError('style end not found')
s=s[:idx]+css+s[idx:]
p.write_text(s)
