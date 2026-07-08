// Avalon Laser — Meta ad creative generator
// Usage: node build.js <creativeId> <size> [photoPath]
//   e.g. node build.js lhr-sandiego 1080x1080 photos/beach-hat.jpg

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const FONT_DIR = path.join(__dirname, 'node_modules');
const font = (pkg, file) => `file://${path.join(FONT_DIR, '@fontsource', pkg, 'files', file)}`;

// ---- Brand ----
const GREEN = '#4E8A50';        // price/accent green sampled from Botox card
const GREEN_DARK = '#2E6B3A';   // wordmark "AVALON" green
const CHARCOAL = '#2b2b2b';

// ---- Offers ----
const OFFERS = {
  lhr: {
    headline: ['LASER HAIR', 'REMOVAL'],
    price: { big: '50% OFF', slash: '/FIRST PACKAGE' },
    photo: 'photos/lhr.jpg',
    textColor: 'white',      // headline color on photo
    priceColor: GREEN,
    logoStyle: 'light',      // light = white wordmark, green = green wordmark
  },
  tirz: {
    headline: ['TIRZEPATIDE'],
    price: { big: '$280', slash: '/FIRST MONTH' },
    sub: '$70 per weekly shot',
    photo: 'photos/tirz.jpg',
    textColor: GREEN,
    priceColor: CHARCOAL,
    logoStyle: 'green',
  },
  cnb: {
    headline: ['CLEAR &', 'BRILLIANT'],
    price: { big: '$380', slash: '/FIRST SESSION' },
    sub: 'Photofractional Laser',
    photo: 'photos/cnb.jpg',
    textColor: 'white',
    priceColor: GREEN,
    logoStyle: 'light',
  },
};

const LOCATIONS = {
  sandiego: 'San Diego',
  carlsbad: 'Carlsbad',
  encinitas: 'Encinitas',
};

function html(offer, location, W, H, photoPath) {
  const photoCss = photoPath
    ? `background-image: url('file://${path.resolve(photoPath)}'); background-size: cover; background-position: center;`
    : `background: linear-gradient(180deg, #cfd8dc 0%, #b3c9cf 45%, #a8bfc4 55%, #d9c9ae 78%, #e2d3b8 100%);`; // placeholder sky/sea/sand
  const scale = W / 1080;
  const logoColor1 = offer.logoStyle === 'green' ? GREEN_DARK : '#ffffff';
  const logoColor2 = offer.logoStyle === 'green' ? GREEN : 'rgba(255,255,255,0.85)';

  return `<!DOCTYPE html><html><head><meta charset="utf-8"><style>
  @font-face { font-family: 'Anton'; src: url('${font('anton', 'anton-latin-400-normal.woff2')}') format('woff2'); }
  @font-face { font-family: 'Oswald'; font-weight: 600; src: url('${font('oswald', 'oswald-latin-600-normal.woff2')}') format('woff2'); }
  @font-face { font-family: 'Oswald'; font-weight: 300; src: url('${font('oswald', 'oswald-latin-300-normal.woff2')}') format('woff2'); }
  @font-face { font-family: 'Inter'; font-weight: 500; src: url('${font('inter', 'inter-latin-500-normal.woff2')}') format('woff2'); }
  @font-face { font-family: 'Inter'; font-weight: 600; src: url('${font('inter', 'inter-latin-600-normal.woff2')}') format('woff2'); }
  * { margin:0; padding:0; box-sizing:border-box; }
  .ad { width:${W}px; height:${H}px; position:relative; overflow:hidden; ${photoCss} }
  .inner { position:absolute; inset:0; transform: scale(${scale}); transform-origin: top left; width:1080px; height:${H / scale}px; }

  .logo { position:absolute; top:72px; left:72px; font-family:'Oswald'; font-size:34px; letter-spacing:1px; }
  .logo .a { font-weight:600; color:${logoColor1}; }
  .logo .l { font-weight:300; color:${logoColor2}; margin-left:2px; }

  .block { position:absolute; left:72px; top:50%; transform:translateY(-38%); }
  .headline { font-family:'Anton'; color:${offer.textColor}; font-size:118px; line-height:1.0;
    letter-spacing:2px; text-shadow:0 2px 14px rgba(0,0,0,0.18); }
  .price { font-family:'Anton'; font-size:56px; color:${offer.priceColor}; margin-top:18px; letter-spacing:1px;
    text-shadow:0 1px 8px rgba(0,0,0,0.12); }
  .price .slash { font-style:italic; }
  .sub { font-family:'Inter'; font-weight:600; font-size:34px; color:${offer.textColor === 'white' ? 'white' : CHARCOAL}; margin-top:14px; text-shadow:0 1px 6px rgba(0,0,0,0.15); }
  .qual { font-family:'Inter'; font-weight:600; font-size:33px; color:${offer.textColor === 'white' ? 'white' : CHARCOAL}; margin-top:16px; text-shadow:0 1px 6px rgba(0,0,0,0.15); }
  .thru { font-family:'Inter'; font-weight:500; font-size:24px; color:${offer.textColor === 'white' ? 'rgba(255,255,255,0.92)' : 'rgba(43,43,43,0.85)'}; margin-top:10px; text-shadow:0 1px 5px rgba(0,0,0,0.12); }
  </style></head><body>
  <div class="ad"><div class="inner">
    <div class="logo"><span class="a">AVALON</span><span class="l">LASER</span></div>
    <div class="block">
      <div class="headline">${offer.headline.join('<br>')}</div>
      <div class="price">${offer.price.big} <span class="slash">/${''}</span><span>${offer.price.slash.replace('/', '')}</span></div>
      ${offer.sub ? `<div class="sub">${offer.sub}</div>` : ''}
      <div class="qual">*New Patients | ${location}</div>
      <div class="thru">Offer good through July 31</div>
    </div>
  </div></div></body></html>`;
}

(async () => {
  const [id, size, photoOverride] = process.argv.slice(2);
  const [offerKey, locKey] = id.split('-');
  const offer = OFFERS[offerKey];
  const location = LOCATIONS[locKey];
  if (!offer || !location) { console.error('unknown creative id', id); process.exit(1); }
  const [W, H] = size.split('x').map(Number);

  const photo = photoOverride || (fs.existsSync(path.join(__dirname, offer.photo)) ? path.join(__dirname, offer.photo) : null);
  const out = path.join(__dirname, 'out', `avalon-${id}-${size}.png`);
  fs.mkdirSync(path.dirname(out), { recursive: true });

  const htmlPath = path.join(__dirname, 'out', `_${id}-${size}.html`);
  fs.writeFileSync(htmlPath, html(offer, location, W, H, photo));
  const browser = await chromium.launch({ executablePath: '/opt/pw-browsers/chromium-1194/chrome-linux/chrome' });
  const page = await browser.newPage({ viewport: { width: W, height: H } });
  await page.goto(`file://${htmlPath}`, { waitUntil: 'networkidle' });
  await page.evaluate(() => document.fonts.ready);
  await page.screenshot({ path: out });
  await browser.close();
  console.log('rendered', out, photo ? `(photo: ${photo})` : '(PLACEHOLDER background)');
})();
