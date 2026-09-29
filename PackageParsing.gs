// Only evidence returned for this offer is usable. Query text, URLs and target sizes are never evidence.
function shoppingSizeEvidence_(row){
  const evidence=[];
  (Array.isArray(row.extensions)?row.extensions:[]).forEach(function(value){
    if(typeof value!=='string')return;
    const text=value.trim();
    if(/^(?:(?:net\s*(?:weight|wt|contents)|package\s*(?:size|weight)|size|weight|quantity|count)\s*[:=]\s*)?(?:\d|half\b|one\b)/i.test(text)&&!/(serving|shipping|delivery|per\s|\/\s*(?:lb|oz)|from|to\b|\bor\b)/i.test(text))evidence.push({source:'extensions',text:text});
  });
  // Snippets can describe serving weights or alternate variants. Use only explicitly labelled package data.
  const snippet=String(row.snippet||'');
  const match=snippet.match(/(?:^|[;\n])\s*((?:net\s*(?:weight|wt|contents)|package\s*(?:size|weight))\s*:\s*[^;\n]+)/i);
  if(match&&!/(serving|shipping|\bor\b|\bto\b|per\s)/i.test(match[1]))evidence.push({source:'snippet',text:match[1]});
  return evidence;
}

function convertPackageUnit_(quantity,unit,target){
  const mass={lb:453.59237,oz:28.349523125,gram:1,kg:1000};
  const volume={gallon:128,fl_oz:1,quart:32,pint:16,liter:33.8140227,ml:0.0338140227};
  if(unit===target)return quantity;
  if(mass[unit]&&mass[target])return quantity*mass[unit]/mass[target];
  if(volume[unit]&&volume[target])return quantity*volume[unit]/volume[target];
  return NaN;
}

function parsePackageText_(input,target){
  let text=String(input||'').toLowerCase().replace(/½/g,'0.5').replace(/¼/g,'0.25').replace(/¾/g,'0.75')
    .replace(/\bhalf\s+(?:a\s+)?gallon\b/g,'0.5 gallon').replace(/\bhalf\s+(?:a\s+)?dozen\b/g,'6 count').replace(/\bone\s+dozen\b/g,'12 count').replace(/\b(\d+)\s+dozen\b/g,function(_,n){return Number(n)*12+' count';}).replace(/\bdozen\b/g,'12 count');
  text=text.replace(/\b(\d+)\s+(\d+)\/(\d+)\b/g,function(_,a,b,c){return Number(a)+Number(b)/Number(c);})
    .replace(/\b(\d+)\/(\d+)\s*(?=lb|oz|gal)/g,function(_,a,b){return Number(a)/Number(b);});
  if(/\d+(?:\.\d+)?\s*(?:-|to)\s*\d+(?:\.\d+)?\s*(?:lb|oz|kg|gallon|count)/.test(text))return{error:'ambiguous_size_range'};
  const re=/(\d+(?:\.\d+)?)\s*[- ]?\s*(fl\.?\s*oz\.?|fluid\s+ounces?|pounds?|lbs?|ounces?|oz|kilograms?|kg|grams?|g|gallons?|gal|quarts?|qt|pints?|pt|liters?|litres?|l|ml|count|ct|kwh)\b/g;
  const found=[];let m;
  while((m=re.exec(text))){
    const token=m[2].replace(/[.\s]/g,'');
    let unit=/^(pound|lb)/.test(token)?'lb':/^(ounce|oz)/.test(token)?'oz':/^(fl|fluid)/.test(token)?'fl_oz':/^(kilogram|kg)/.test(token)?'kg':/^(gram|g$)/.test(token)?'gram':/^(gal)/.test(token)?'gallon':/^(qt|quart)/.test(token)?'quart':/^(pt|pint)/.test(token)?'pint':/^(liter|litre|l$)/.test(token)?'liter':token==='ml'?'ml':/^(count|ct)/.test(token)?'count':'kwh';
    // Nutrition or per-unit prices are not a sellable package quantity.
    if(/(?:per|\$)\s*$/.test(text.slice(Math.max(0,m.index-10),m.index)))return{error:'ambiguous_size'};
    const quantity=convertPackageUnit_(Number(m[1]),unit,target);
    // Ignore stick/count descriptors when an explicit mass exists, but never convert mass to egg count.
    if(isFinite(quantity))found.push({quantity:quantity,unit:target,index:m.index});
  }
  if(!found.length)return{quantity:NaN,unit:'',error:''};
  let multiplier=1;
  const prefix=text.slice(0,found[0].index);
  const times=prefix.match(/(\d+)\s*[x×]\s*$/);
  const pack=text.match(/(?:pack|case)\s+of\s+(\d+)|(\d+)\s*[- ]?(?:pack|pk)\b|(\d+)\s+per\s+case/);
  if(times)multiplier=Number(times[1]);
  if(pack){const count=Number(pack[1]||pack[2]||pack[3]);if(times&&count!==multiplier)return{error:'ambiguous_multipack'};multiplier=count;}
  if(/\b(case|multipack|club pack)\b/.test(text)&&!pack&&!times)return{error:'ambiguous_multipack'};
  const base=found[0].quantity;
  if(found.some(function(x){return Math.abs(x.quantity-base)/base>0.02;}))return{error:'conflicting_size'};
  return{quantity:base*multiplier,unit:target,error:''};
}

function candidatePackage_(candidate,item){
  const titleParts=String(candidate.title||'').split('|');
  if(titleParts.filter(function(text){return finitePositive_(parsePackageText_(text,item.unit).quantity);}).length>1)return{quantity:NaN,unit:'',error:'ambiguous_product_bundle',source:'title'};
  const evidence=[{source:'title',text:String(candidate.title||'')}].concat(candidate.sizeEvidence||[]);
  const parsed=evidence.map(function(e){return Object.assign({source:e.source},parsePackageText_(e.text,item.unit));});
  const invalid=parsed.filter(function(p){return p.error;})[0];
  if(invalid)return{quantity:NaN,unit:'',error:invalid.error,source:invalid.source};
  const sizes=parsed.filter(function(p){return finitePositive_(p.quantity);});
  if(!sizes.length)return{quantity:NaN,unit:'',source:'none'};
  const base=sizes[0];
  if(sizes.some(function(p){return Math.abs(p.quantity-base.quantity)/base.quantity>0.02;}))return{quantity:NaN,unit:'',error:'conflicting_size',source:'conflict'};
  return base;
}
