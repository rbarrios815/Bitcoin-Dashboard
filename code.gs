const SHEET_ID = '1WW941MlbDhTTm6UWdqEiVt9aPeKEXb-8k_jJUR9blLg';

function doGet() {
  return HtmlService.createHtmlOutputFromFile('index')
    .setTitle('Creative Bitcoin Dashboard')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

function getDashboardData() {
  const spreadsheet = SpreadsheetApp.openById(SHEET_ID);
  const sheet = spreadsheet.getSheets()[0];
  const summary = sheet ? readKeyValuePairs(sheet, 40) : {};

  const trends = readTableByName(spreadsheet, 'Trends');
  const research = readTableByName(spreadsheet, 'Research');
  const highlights = readTableByName(spreadsheet, 'Highlights');
  const interesting = readTableByName(spreadsheet, 'MostInteresting');
  const emails = readTableByName(spreadsheet, 'SatoshiEmails');
  const mining = readTableByName(spreadsheet, 'Mining');
  const hashrate = readTableByName(spreadsheet, 'Hashrate');

  const price = normalizeNumber(summary.live_price || summary.price || summary['btc_live_price']);
  const priceChange = normalizeNumber(summary.price_change_24h || summary.price_change || summary['price_change_24h']);
  const satoshiQuote = summary.satoshi_info || summary['satoshi_info'] || summary['satoshi_quote'] || '';
  const satoshiEmails = summary.satoshi_emails || summary['satoshi_emails'] || '';
  const totalBitcoin = normalizeNumber(summary.total_bitcoin_outstanding || summary.total_bitcoin || summary['total_bitcoin_outstanding']) || 19700000;
  const hashrateValue = normalizeNumber(summary.hashrate || summary['hashrate']) || '';

  return {
    price,
    priceChange,
    satoshiQuote,
    satoshiEmails,
    totalBitcoin,
    hashrateValue,
    summary,
    trends,
    research,
    highlights,
    interesting,
    emails,
    mining,
    hashrate
  };
}

function readKeyValuePairs(sheet, maxRows) {
  const lastRow = Math.min(sheet.getLastRow(), maxRows);
  if (lastRow < 1) return {};
  const values = sheet.getRange(1, 1, lastRow, 2).getValues();
  return values.reduce((acc, row) => {
    const key = String(row[0] || '').trim();
    if (!key) return acc;
    const normalized = key.toLowerCase().replace(/\s+/g, '_');
    acc[normalized] = row[1];
    return acc;
  }, {});
}

function readTableByName(spreadsheet, name) {
  const sheet = spreadsheet.getSheetByName(name);
  if (!sheet) return [];
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return [];
  const headers = data[0].map((header) => String(header || '').trim());
  return data.slice(1).filter((row) => row.some((cell) => cell !== '')).map((row) => {
    const entry = {};
    headers.forEach((header, index) => {
      if (header) {
        entry[header] = row[index];
      }
    });
    return entry;
  });
}

function normalizeNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return value;
  const cleaned = String(value).replace(/[^0-9.-]/g, '');
  if (!cleaned) return null;
  const parsed = Number(cleaned);
  return Number.isNaN(parsed) ? null : parsed;
}

// Place a BTC live price formula in the sheet, for example in cell B1:
// =GOOGLEFINANCE("CURRENCY:BTCUSD")
// Then set A1 to "live_price" so the dashboard can read it automatically.
