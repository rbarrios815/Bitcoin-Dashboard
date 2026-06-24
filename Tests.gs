function testMeasurementContract(){
  const basket=[{usd:4,sats:8000},{usd:5,sats:10000}];
  const totalUsd=basket.reduce(function(sum,row){return sum+row.usd;},0);
  if(totalUsd!==9)throw new Error('Basket must sum package prices.');
  if(isCoreId_('gold'))throw new Error('Gold cannot be a grocery basket item.');
  const butter=validateCandidate_(catalogById_('butter'),{title:'Store Unsalted Butter 16 oz',price:4.99,vendor:'Grocery Store',url:'https://example.com',provider:'test'});
  if(!butter.pass)throw new Error('Unsalted butter matcher regression: '+butter.failReason);
  const outliers=removeOutliers_([{normalizedPrice:1.64},{normalizedPrice:3.98},{normalizedPrice:9.99},{normalizedPrice:118.85}]);
  if(outliers.some(function(row){return row.normalizedPrice===118.85;}))throw new Error('Outlier filter regression.');
  return{ok:true,version:PP_VERSION};
}
