/**
 * Bitcoin Purchasing Power Dashboard v2
 * Google Apps Script backend.
 *
 * Measurement contract:
 * - The grocery basket is a SUM of one standardized package per active grocery item.
 * - Gold, silver, electricity, $10, and 10,000 sats are reference series only.
 * - Current retrieval failures may carry forward the last valid USD package price,
 *   but the observation remains visibly stale and receives reduced confidence credit.
 */

const PP_VERSION = '2.0.0';
const HISTORY_HEADER = [
  'timestamp','btc_usd','item_id','item_name','query','canonical_query',
  'item_description','raw_vendor_title','source_item_description','usd','sats',
  'normalized_price','normalized_unit','basket_index_usd','basket_index_sats',
  'price_source','price_vendor','source_url','is_stale','match_score',
  'validation_status','fail_reason','matched_source_count','used_source_count',
  'aggregation_method','selected_vendors','selected_source_urls',
  'selected_match_scores','aggregated_normalized_prices','is_multi_source'
];
const RAW_HEADER = [
  'timestamp','item_id','vendor','raw_vendor_title','raw_price','parsed_quantity',
  'parsed_unit','normalized_price','normalized_unit','pass_fail','fail_reason',
  'match_score','source_url','price_source','selected_for_aggregate'
];
const REFERENCE_IDS = ['gold','silver','mwh','cash10','sats10000'];
const MARKETPLACE_RE = /(ebay|etsy|whatnot|shop\s*lc|alibaba|aliexpress|temu)/i;
const BLS_ELECTRICITY_SERIES = 'APU000072610';
const BLS_ELECTRICITY_URL = 'https://data.bls.gov/timeseries/APU000072610';

const CATALOG = [
  {id:'apples',name:'Apples',description:'Honeycrisp apples 3 lb bag',query:'honeycrisp apples 3 lb bag',required:['honeycrisp','apple'],excluded:['juice','cider','slices'],unit:'lb',quantity:3,category:'produce',bounds:[0.25,8]},
  {id:'bananas',name:'Bananas',description:'Bananas 1 lb',query:'bananas 1 lb',required:['banana'],excluded:['chips','baby food'],unit:'lb',quantity:1,category:'produce',bounds:[0.15,4]},
  {id:'eggs',name:'Eggs',description:'Grade A large eggs 12 count',query:'grade a large eggs 12 count',required:['egg'],excluded:['liquid','substitute'],unit:'count',quantity:12,category:'dairy',bounds:[0.05,2]},
  {id:'milk',name:'Milk',description:'Whole milk 1 gallon',query:'whole milk 1 gallon',required:['whole','milk'],excluded:['almond','oat','soy','skim','2%'],unit:'gallon',quantity:1,category:'dairy',bounds:[1,15]},
  {id:'butter',name:'Butter',description:'Unsalted butter 16 oz',query:'unsalted butter 16 oz',required:['unsalted','butter'],excluded:['spread','margarine'],unit:'oz',quantity:16,category:'dairy',bounds:[0.08,2]},
  {id:'bread',name:'Bread',description:'Sandwich bread 20 oz loaf',query:'sandwich bread 20 oz loaf',required:['bread','sandwich'],excluded:['bun','bagel','roll','gluten free'],unit:'oz',quantity:20,category:'bakery',bounds:[0.03,1.5]},
  {id:'rice',name:'Rice',description:'Long grain white rice 5 lb bag',query:'long grain white rice 5 lb bag',required:['rice','long','white'],excluded:['brown','cauliflower','minute'],unit:'lb',quantity:5,category:'pantry',bounds:[0.15,8]},
  {id:'chicken',name:'Chicken',description:'Boneless skinless chicken breast 2 lb',query:'boneless skinless chicken breast 2 lb',required:['chicken','breast','boneless','skinless'],excluded:['whole','thigh','wing','drumstick','tender'],unit:'lb',quantity:2,category:'meat',bounds:[1,20]},
  {id:'ground_beef',name:'Ground Beef',description:'Ground beef 80/20 1 lb',query:'ground beef 80/20 1 lb',required:['ground','beef'],excluded:['patty','wagyu'],unit:'lb',quantity:1,category:'meat',bounds:[2,30]},
  {id:'potatoes',name:'Potatoes',description:'Russet potatoes 5 lb bag',query:'russet potatoes 5 lb bag',required:['russet','potato'],excluded:['yukon','gold','red','sweet'],unit:'lb',quantity:5,category:'produce',bounds:[0.10,5]},
  {id:'yellow_onions',name:'Yellow Onions',description:'Yellow onions 3 lb bag',query:'yellow onions 3 lb bag',required:['yellow','onion'],excluded:['red','sweet','shallot'],unit:'lb',quantity:3,category:'produce',bounds:[0.10,5]},
  {id:'salt',name:'Salt',description:'Iodized table salt 26 oz',query:'iodized table salt 26 oz',required:['salt','iodized'],excluded:['kosher','sea salt','himalayan'],unit:'oz',quantity:26,category:'pantry',bounds:[0.01,1]},
  {id:'gold',name:'Gold',description:'Gold 0.1 gram bar',query:'0.1 gram gold bar',required:['gold'],excluded:[],unit:'gram',quantity:0.1,category:'reference',bounds:[100,5000]},
  {id:'silver',name:'Silver',description:'Silver 1 gram bar',query:'1 gram silver bar',required:['silver'],excluded:[],unit:'gram',quantity:1,category:'reference',bounds:[0.25,100]},
  {id:'mwh',name:'Electricity',description:'Electricity 5 kWh',query:'electricity 5 kWh',required:['electricity'],excluded:[],unit:'kwh',quantity:5,category:'reference',bounds:[0.01,2]},
  {id:'cash10',name:'$10',description:'$10',query:'$10',required:[],excluded:[],unit:'usd',quantity:10,category:'reference'},
  {id:'sats10000',name:'10,000 sats',description:'10,000 satoshis',query:'10000 satoshis',required:[],excluded:[],unit:'sats',quantity:10000,category:'reference'}
];

function doGet() {
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle('Bitcoin Purchasing Power Dashboard')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function setupOnce() {
  const props = getProps_();
  validateCollectionProps_(props);
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const history = ensureSheet_(ss, props.dataSheetName, HISTORY_HEADER);
  ensureSheet_(ss, props.rawOffersSheetName, RAW_HEADER);
  installDailyTrigger_();
  return {ok:true,version:PP_VERSION,sheet:history.getName(),triggerInstalled:true};
}

function installDailyTrigger_() {
  ScriptApp.getProjectTriggers().forEach(function(trigger) {
    if (trigger.getHandlerFunction() === 'recordSnapshotDaily') ScriptApp.deleteTrigger(trigger);
  });
  ScriptApp.newTrigger('recordSnapshotDaily').timeBased().everyDays(1).atHour(8).create();
}
function recordSnapshotDaily() { return recordSnapshot(); }

function getConfig() {
  const props = getProps_();
  const active = getActiveCatalog_(props);
  return {
    version: PP_VERSION,
    location: props.serpApiLocation || 'United States',
    dataSheetName: props.dataSheetName,
    basketItems: active.filter(isCoreItem_).map(publicItem_),
    referenceItems: active.filter(function(item){ return !isCoreItem_(item); }).map(publicItem_)
  };
}
