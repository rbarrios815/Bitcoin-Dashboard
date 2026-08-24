function testMeasurementContract(){
  const basket=[{usd:4,sats:8000},{usd:5,sats:10000}];
  const totalUsd=basket.reduce(function(sum,row){return sum+row.usd;},0);
  if(totalUsd!==9)throw new Error('Basket must sum package prices.');
  if(isCoreId_('gold'))throw new Error('Gold cannot be a grocery basket item.');
  const butter=validateCandidate_(catalogById_('butter'),{title:'Store Unsalted Butter 16 oz',price:4.99,vendor:'Grocery Store',url:'https://example.com',provider:'test'});
  if(!butter.pass)throw new Error('Unsalted butter matcher regression: '+butter.failReason);
  const outliers=removeOutliers_([{normalizedPrice:1.64},{normalizedPrice:3.98},{normalizedPrice:9.99},{normalizedPrice:118.85}]);
  if(outliers.some(function(row){return row.normalizedPrice===118.85;}))throw new Error('Outlier filter regression.');
  testSerpApiBudget_();
  return{ok:true,version:PP_VERSION};
}

function testSerpApiBudget_(){
  if(calculateSerpApiAllowance_(220,0,6,12)!==6)throw new Error('Daily SerpApi cap regression.');
  if(calculateSerpApiAllowance_(220,218,6,12)!==2)throw new Error('Monthly SerpApi cap regression.');
  if(calculateSerpApiAllowance_(220,220,6,12)!==0)throw new Error('Exhausted local budget regression.');
  const items=[];
  for(let i=0;i<12;i++)items.push({id:'item_'+i});
  const first=selectSerpApiRotation_(items,6,0);
  const second=selectSerpApiRotation_(items,6,1);
  const ids={};
  first.concat(second).forEach(function(item){ids[item.id]=true;});
  if(Object.keys(ids).length!==12)throw new Error('Two-day rotation must cover all 12 default SerpApi items.');
  if(!isSerpApiExhaustedResponse_(429,''))throw new Error('HTTP 429 must block further SerpApi calls for the month.');
  if(!isSerpApiExhaustedResponse_(403,'Your searches are exhausted'))throw new Error('Provider exhaustion message must be detected.');
  if(isSerpApiExhaustedResponse_(500,'temporary error'))throw new Error('Transient provider errors must not block the month.');
  return{ok:true};
}
