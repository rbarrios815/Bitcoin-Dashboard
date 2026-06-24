function getActiveCatalog_(props) {
  const configured={};
  String(props.itemList||'').split(',').map(function(x){return x.trim();}).filter(Boolean).forEach(function(token){const parts=token.indexOf('|')>=0?token.split('|').map(function(x){return x.trim();}):[slug_(token),'',token];configured[String(parts[0]||'').toLowerCase()]={name:parts[1],query:parts[2]};});
  let active=CATALOG.filter(function(item){return configured[item.id]||REFERENCE_IDS.indexOf(item.id)>=0;}).map(function(item){const over=configured[item.id]||{};return Object.assign({},item,{name:over.name||item.name,query:item.query});});
  const core=active.filter(isCoreItem_);
  if(!core.length) active=CATALOG.filter(function(item){return ['apples','bananas','eggs','milk','butter','bread','rice','chicken','ground_beef','potatoes'].indexOf(item.id)>=0||REFERENCE_IDS.indexOf(item.id)>=0;});
  return active;
}
function publicItem_(item){return{id:item.id,name:item.name,description:item.description,quantity:item.quantity,unit:item.unit,category:item.category};}
function isCoreItem_(item){return item&&REFERENCE_IDS.indexOf(item.id)<0;}
function isCoreId_(id){return REFERENCE_IDS.indexOf(String(id||'').toLowerCase())<0;}
function catalogById_(id){return CATALOG.filter(function(item){return item.id===id;})[0]||null;}
