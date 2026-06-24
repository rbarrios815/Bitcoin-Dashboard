function getFrontendPartial(name){
  if(['Styles','App'].indexOf(String(name))<0)throw new Error('Unknown frontend partial.');
  return HtmlService.createHtmlOutputFromFile(String(name)).getContent();
}
