/**
 * ⚽ محرك التغذية الحية - Live Data Sync Engine
 * يجلب بيانات حقيقية من worldcup26.ir كل 60 ثانية
 * ويرفعها إلى Firebase → يُحدّث التطبيق لحظياً لجميع المستخدمين
 *
 * Architecture:
 *   worldcup26.ir (Real API) → sync.mjs (every 60s) → Firebase → App (real-time)
 */

import { initializeApp } from 'firebase/app';
import { getDatabase, ref, set, get } from 'firebase/database';
import axios from 'axios';
import https from 'https';
import express from 'express';

// ── Cloud Health Check Server (Render/Heroku) ──────────────────────
const PORT = process.env.PORT || 8080;
const server = express();

server.get('/', (req, res) => {
  res.send('⚽ World Cup 2026 Live Sync Engine is running!');
});
server.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK', uptime: process.uptime() });
});

server.listen(PORT, () => {
  console.log(`🌍 Health Check Server listening on port ${PORT}`);
});

// ── Firebase Config ──────────────────────────────────────────────
const firebaseConfig = {
  apiKey: "AIzaSyB3-zgU0x36IYkggp1rPfqARMxpR777g6g",
  authDomain: "worldcup2026-live.firebaseapp.com",
  databaseURL: "https://worldcup2026-live-default-rtdb.firebaseio.com",
  projectId: "worldcup2026-live",
  storageBucket: "worldcup2026-live.firebasestorage.app",
  messagingSenderId: "83491561729",
  appId: "1:83491561729:web:624868e31b7ce29d9e7b00"
};

const app = initializeApp(firebaseConfig);
const db = getDatabase(app);

// ── Constants ────────────────────────────────────────────────────
const API_BASE = 'https://worldcup26.ir';
const SYNC_INTERVAL_MS = 60_000; // 60 seconds
const GROUP_STAGE_ONLY = true;   // Only sync group stage matches (type: "group")

// ── Team name mapping (API English → Arabic) ─────────────────────
const TEAM_NAMES_AR = {
  'Mexico': 'المكسيك', 'South Africa': 'جنوب أفريقيا', 'South Korea': 'كوريا الجنوبية',
  'Czech Republic': 'التشيك', 'Canada': 'كندا', 'Bosnia and Herzegovina': 'البوسنة',
  'United States': 'الولايات المتحدة', 'Paraguay': 'باراغواي', 'Haiti': 'هايتي',
  'Scotland': 'اسكتلندا', 'Australia': 'أستراليا', 'Turkey': 'تركيا',
  'Brazil': 'البرازيل', 'Morocco': 'المغرب', 'Qatar': 'قطر', 'Switzerland': 'سويسرا',
  'Ivory Coast': 'ساحل العاج', 'Ecuador': 'الإكوادور', 'Germany': 'ألمانيا',
  'Curaçao': 'كوراساو', 'Netherlands': 'هولندا', 'Japan': 'اليابان',
  'Sweden': 'السويد', 'Tunisia': 'تونس', 'Belgium': 'بلجيكا', 'Egypt': 'مصر',
  'Iran': 'إيران', 'New Zealand': 'نيوزيلندا', 'Spain': 'إسبانيا',
  'Cape Verde': 'الرأس الأخضر', 'Saudi Arabia': 'السعودية', 'Uruguay': 'أوروغواي',
  'France': 'فرنسا', 'Senegal': 'السنغال', 'Iraq': 'العراق', 'Norway': 'النرويج',
  'Argentina': 'الأرجنتين', 'Algeria': 'الجزائر', 'Austria': 'النمسا',
  'Jordan': 'الأردن', 'Portugal': 'البرتغال',
  'Democratic Republic of the Congo': 'الكونغو الديمقراطية',
  'Uzbekistan': 'أوزبكستان', 'Colombia': 'كولومبيا', 'England': 'إنجلترا',
  'Croatia': 'كرواتيا', 'Ghana': 'غانا', 'Panama': 'بنما',
};

// ── Team flags ────────────────────────────────────────────────────
const TEAM_FLAGS = {
  'Mexico': '🇲🇽', 'South Africa': '🇿🇦', 'South Korea': '🇰🇷', 'Czech Republic': '🇨🇿',
  'Canada': '🇨🇦', 'Bosnia and Herzegovina': '🇧🇦', 'United States': '🇺🇸',
  'Paraguay': '🇵🇾', 'Haiti': '🇭🇹', 'Scotland': '🏴󠁧󠁢󠁳󠁣󠁴󠁿', 'Australia': '🇦🇺',
  'Turkey': '🇹🇷', 'Brazil': '🇧🇷', 'Morocco': '🇲🇦', 'Qatar': '🇶🇦',
  'Switzerland': '🇨🇭', 'Ivory Coast': '🇨🇮', 'Ecuador': '🇪🇨', 'Germany': '🇩🇪',
  'Curaçao': '🇨🇼', 'Netherlands': '🇳🇱', 'Japan': '🇯🇵', 'Sweden': '🇸🇪',
  'Tunisia': '🇹🇳', 'Belgium': '🇧🇪', 'Egypt': '🇪🇬', 'Iran': '🇮🇷',
  'New Zealand': '🇳🇿', 'Spain': '🇪🇸', 'Cape Verde': '🇨🇻', 'Saudi Arabia': '🇸🇦',
  'Uruguay': '🇺🇾', 'France': '🇫🇷', 'Senegal': '🇸🇳', 'Iraq': '🇮🇶',
  'Norway': '🇳🇴', 'Argentina': '🇦🇷', 'Algeria': '🇩🇿', 'Austria': '🇦🇹',
  'Jordan': '🇯🇴', 'Portugal': '🇵🇹',
  'Democratic Republic of the Congo': '🇨🇩',
  'Uzbekistan': '🇺🇿', 'Colombia': '🇨🇴', 'England': '🏴󠁧󠁢󠁥󠁮󠁧󠁿',
  'Croatia': '🇭🇷', 'Ghana': '🇬🇭', 'Panama': '🇵🇦',
};

// ── Team IDs (for computedTeams in the app) ───────────────────────
const TEAM_IDS = {
  'Mexico': 'mex', 'South Africa': 'rsa', 'South Korea': 'kor', 'Czech Republic': 'cze',
  'Canada': 'can', 'Bosnia and Herzegovina': 'bih', 'United States': 'usa',
  'Paraguay': 'par', 'Haiti': 'hai', 'Scotland': 'sco', 'Australia': 'aus',
  'Turkey': 'tur', 'Brazil': 'bra', 'Morocco': 'mar', 'Qatar': 'qat',
  'Switzerland': 'sui', 'Ivory Coast': 'civ', 'Ecuador': 'ecu', 'Germany': 'ger',
  'Curaçao': 'cur', 'Netherlands': 'ned', 'Japan': 'jpn', 'Sweden': 'swe',
  'Tunisia': 'tun', 'Belgium': 'bel', 'Egypt': 'egy', 'Iran': 'irn',
  'New Zealand': 'nzl', 'Spain': 'esp', 'Cape Verde': 'cpv', 'Saudi Arabia': 'ksa',
  'Uruguay': 'ury', 'France': 'fra', 'Senegal': 'sen', 'Iraq': 'irq',
  'Norway': 'nor', 'Argentina': 'arg', 'Algeria': 'alg', 'Austria': 'aut',
  'Jordan': 'jor', 'Portugal': 'por',
  'Democratic Republic of the Congo': 'cod',
  'Uzbekistan': 'uzb', 'Colombia': 'col', 'England': 'eng',
  'Croatia': 'cro', 'Ghana': 'gha', 'Panama': 'pan',
};

// ── Stadium names (Arabic) ────────────────────────────────────────
const STADIUMS_AR = {
  1: { nameAr: 'استاد أزتيكا', nameEn: 'Estadio Azteca', cityAr: 'مكسيكو سيتي', cityEn: 'Mexico City' },
  2: { nameAr: 'استاد أكرون', nameEn: 'Estadio Akron', cityAr: 'غوادالاخارا', cityEn: 'Guadalajara' },
  3: { nameAr: 'استاد BBVA', nameEn: 'Estadio BBVA', cityAr: 'مونتيري', cityEn: 'Monterrey' },
  4: { nameAr: 'استاد BC Place', nameEn: 'BC Place', cityAr: 'فانكوفر', cityEn: 'Vancouver' },
  5: { nameAr: 'استاد تورنتو', nameEn: 'BMO Field', cityAr: 'تورنتو', cityEn: 'Toronto' },
  6: { nameAr: 'استاد أتلانتا', nameEn: 'Mercedes-Benz Stadium', cityAr: 'أتلانتا', cityEn: 'Atlanta' },
  7: { nameAr: 'استاد ميتلايف', nameEn: 'MetLife Stadium', cityAr: 'نيويورك', cityEn: 'New York/New Jersey' },
  8: { nameAr: 'استاد هارد روك', nameEn: 'Hard Rock Stadium', cityAr: 'ميامي', cityEn: 'Miami' },
  9: { nameAr: 'استاد جيليت', nameEn: 'Gillette Stadium', cityAr: 'بوسطن', cityEn: 'Boston' },
  10: { nameAr: 'استاد دالاس', nameEn: "AT&T Stadium", cityAr: 'دالاس', cityEn: 'Dallas' },
  11: { nameAr: 'استاد سوفاي', nameEn: 'SoFi Stadium', cityAr: 'لوس أنجلوس', cityEn: 'Los Angeles' },
  12: { nameAr: 'استاد لومين فيلد', nameEn: 'Lumen Field', cityAr: 'سياتل', cityEn: 'Seattle' },
  13: { nameAr: 'استاد كانساس سيتي', nameEn: 'Arrowhead Stadium', cityAr: 'كانساس سيتي', cityEn: 'Kansas City' },
  14: { nameAr: 'استاد سان فرانسيسكو', nameEn: "Levi's Stadium", cityAr: 'سان فرانسيسكو', cityEn: 'San Francisco' },
  15: { nameAr: 'استاد دالاس NRG', nameEn: 'NRG Stadium', cityAr: 'هيوستن', cityEn: 'Houston' },
  16: { nameAr: 'استاد فيلادلفيا', nameEn: 'Lincoln Financial Field', cityAr: 'فيلادلفيا', cityEn: 'Philadelphia' },
};

// ── Helper: build team object ─────────────────────────────────────
function buildTeam(nameEn, group) {
  return {
    id: TEAM_IDS[nameEn] || nameEn.toLowerCase().replace(/\s+/g, '_').substring(0, 3),
    nameAr: TEAM_NAMES_AR[nameEn] || nameEn,
    nameEn,
    flag: TEAM_FLAGS[nameEn] || '🏳️',
    group,
    played: 0, won: 0, drawn: 0, lost: 0, points: 0, goalsFor: 0, goalsAgainst: 0,
  };
}

// ── Helper: map API status to app status ──────────────────────────
function mapStatus(game) {
  if (game.finished === 'TRUE') return 'FINISHED';
  if (game.time_elapsed && game.time_elapsed !== 'notstarted' && game.time_elapsed !== 'finished') return 'LIVE';
  return 'UPCOMING';
}

// ── Helper: parse date from API format "MM/DD/YYYY HH:mm" ─────────
function parseDate(localDate) {
  // Format: "06/11/2026 13:00"
  const [datePart, timePart] = localDate.split(' ');
  const [mm, dd, yyyy] = datePart.split('/');
  return { date: `${yyyy}-${mm}-${dd}`, time: timePart || '00:00' };
}

// ── News Mock Engine ──────────────────────────────────────────────
const SAMPLE_NEWS = [
  { titleAr: 'استعدادات قوية للأخضر السعودي قبل المواجهة المرتقبة', titleEn: 'Saudi Arabia intensifies preparations before the big clash', summaryAr: 'صرح المدرب بجاهزية جميع اللاعبين للمباراة الافتتاحية.', summaryEn: 'Coach confirms all players are ready for the opening match.' },
  { titleAr: 'المغرب يعول على خط وسطه في مواجهة البرازيل', titleEn: 'Morocco relies on its midfield against Brazil', summaryAr: 'مواجهة تكتيكية منتظرة بين أسود الأطلس وراقصي السامبا.', summaryEn: 'A tactical showdown expected between the Atlas Lions and Samba Dancers.' },
  { titleAr: 'إصابة مفاجئة لنجم المنتخب الفرنسي', titleEn: 'Sudden injury for French star', summaryAr: 'شكوك حول مشاركة مبابي في المباراة القادمة بسبب شد عضلي.', summaryEn: 'Doubts over Mbappe participation due to muscle strain.' },
  { titleAr: 'الفيفا يعلن بيع جميع تذاكر الافتتاح', titleEn: 'FIFA announces all opening tickets sold out', summaryAr: 'حضور جماهيري غفير متوقع في ملعب أزتيكا.', summaryEn: 'Massive attendance expected at Estadio Azteca.' },
  { titleAr: 'مفاجأة مدوية في تشكيلة المنتخب الأرجنتيني', titleEn: 'Massive surprise in Argentina lineup', summaryAr: 'استبعاد أحد أبرز اللاعبين من القائمة الأساسية للمباراة.', summaryEn: 'One of the top players excluded from the starting lineup.' }
];

let lastNewsSync = 0;
const NEWS_SYNC_INTERVAL_MS = 2 * 60 * 1000; // Update news every 2 minutes

async function syncNews() {
  const now = Date.now();
  if (now - lastNewsSync < NEWS_SYNC_INTERVAL_MS) return; 
  lastNewsSync = now;

  const timestamp = new Date().toLocaleTimeString('ar-SA');
  console.log(`\n[${timestamp}] 📰 جاري مزامنة الأخبار الحية...`);
  try {
    // 💡 ملاحظة: يمكن تبديل هذا بطلب حقيقي لـ News API أو RSS Feed
    const randomNews = SAMPLE_NEWS.sort(() => 0.5 - Math.random()).slice(0, 3).map((item, index) => ({
      id: `news_${Date.now()}_${index}`,
      titleAr: item.titleAr,
      titleEn: item.titleEn,
      summaryAr: item.summaryAr,
      summaryEn: item.summaryEn,
      contentAr: item.summaryAr + ' سيتم الإعلان عن تفاصيل إضافية في المؤتمر الصحفي لاحقاً.',
      contentEn: item.summaryEn + ' Additional details will be announced at the press conference later.',
      image: index === 0 ? 'https://images.unsplash.com/photo-1508098682722-e99c43a406b2?w=800&auto=format&fit=crop&q=60' : 'https://images.unsplash.com/photo-1518063319789-7217e6706b04?w=800&auto=format&fit=crop&q=60',
      date: new Date().toISOString().split('T')[0],
      category: 'تحديث آلي ⚡',
      reads: Math.floor(Math.random() * 5000) + 500,
      isBreaking: index === 0 // الأول دائماً عاجل
    }));

    await set(ref(db, 'news'), randomNews);
    console.log(`✅ تم تحديث الأخبار بنجاح: ${randomNews.length} مقالات`);
  } catch (err) {
    console.error(`❌ خطأ في جلب الأخبار: ${err.message}`);
  }
}

// ── Helper: get Star Player for Goals (Distributed) ─────────────────
const TEAM_STARS = {
  'ksa': [
    { ar: 'سالم الدوسري', en: 'Salem Al-Dawsari', assistAr: 'فراس البريكان', assistEn: 'Firas Al-Buraikan' },
    { ar: 'فراس البريكان', en: 'Firas Al-Buraikan', assistAr: 'سالم الدوسري', assistEn: 'Salem Al-Dawsari' },
    { ar: 'عبدالرحمن غريب', en: 'Abdulrahman Ghareeb', assistAr: 'سعود عبدالحميد', assistEn: 'Saud Abdulhamid' }
  ],
  'mar': [
    { ar: 'حكيم زياش', en: 'Hakim Ziyech', assistAr: 'أشرف حكيمي', assistEn: 'Achraf Hakimi' },
    { ar: 'يوسف النصيري', en: 'Youssef En-Nesyri', assistAr: 'براهيم دياز', assistEn: 'Brahim Diaz' },
    { ar: 'براهيم دياز', en: 'Brahim Diaz', assistAr: 'سفيان أمرابط', assistEn: 'Sofyan Amrabat' }
  ],
  'egy': [
    { ar: 'محمد صلاح', en: 'Mohamed Salah', assistAr: 'مصطفى محمد', assistEn: 'Mostafa Mohamed' },
    { ar: 'مصطفى محمد', en: 'Mostafa Mohamed', assistAr: 'تريزيجيه', assistEn: 'Trezeguet' },
    { ar: 'تريزيجيه', en: 'Trezeguet', assistAr: 'عمر مرموش', assistEn: 'Omar Marmoush' }
  ],
  'arg': [
    { ar: 'ليونيل ميسي', en: 'Lionel Messi', assistAr: 'دي ماريا', assistEn: 'Di Maria' },
    { ar: 'لاوتارو مارتينيز', en: 'Lautaro Martinez', assistAr: 'جوليان ألفاريز', assistEn: 'Julian Alvarez' },
    { ar: 'جوليان ألفاريز', en: 'Julian Alvarez', assistAr: 'إنزو فرنانديز', assistEn: 'Enzo Fernandez' }
  ],
  'bra': [
    { ar: 'فينيسيوس جونيور', en: 'Vinicius Jr', assistAr: 'رودريغو', assistEn: 'Rodrygo' },
    { ar: 'رودريغو', en: 'Rodrygo', assistAr: 'فينيسيوس جونيور', assistEn: 'Vinicius Jr' },
    { ar: 'رافينيا', en: 'Raphinha', assistAr: 'لوكاس باكيتا', assistEn: 'Lucas Paqueta' }
  ],
  'fra': [
    { ar: 'مبابي', en: 'Mbappe', assistAr: 'جريزمان', assistEn: 'Griezmann' },
    { ar: 'جريزمان', en: 'Griezmann', assistAr: 'ديمبيلي', assistEn: 'Dembele' },
    { ar: 'جيرو', en: 'Giroud', assistAr: 'مبابي', assistEn: 'Mbappe' }
  ],
  'esp': [
    { ar: 'لامين يامال', en: 'Lamine Yamal', assistAr: 'بيدري', assistEn: 'Pedri' },
    { ar: 'ألفارو موراتا', en: 'Alvaro Morata', assistAr: 'داني أولمو', assistEn: 'Dani Olmo' },
    { ar: 'داني أولمو', en: 'Dani Olmo', assistAr: 'لامين يامال', assistEn: 'Lamine Yamal' }
  ],
  'eng': [
    { ar: 'جود بيلينجهام', en: 'Jude Bellingham', assistAr: 'ساكا', assistEn: 'Saka' },
    { ar: 'هاري كين', en: 'Harry Kane', assistAr: 'فودين', assistEn: 'Foden' },
    { ar: 'بوكايو ساكا', en: 'Bukayo Saka', assistAr: 'ترينت ألكسندر-أرنولد', assistEn: 'Trent Alexander-Arnold' }
  ],
  'por': [
    { ar: 'كريستيانو رونالدو', en: 'Cristiano Ronaldo', assistAr: 'برونو فيرنانديز', assistEn: 'Bruno Fernandes' },
    { ar: 'برونو فيرنانديز', en: 'Bruno Fernandes', assistAr: 'برناردو سيلفا', assistEn: 'Bernardo Silva' },
    { ar: 'جواو فيليكس', en: 'Joao Felix', assistAr: 'رفائيل لياو', assistEn: 'Rafael Leao' }
  ],
  'ger': [
    { ar: 'جمال موسيالا', en: 'Jamal Musiala', assistAr: 'فيرتز', assistEn: 'Wirtz' },
    { ar: 'فلوريان فيرتز', en: 'Florian Wirtz', assistAr: 'غوندوغان', assistEn: 'Gundogan' },
    { ar: 'كاي هافيرتز', en: 'Kai Havertz', assistAr: 'موسيالا', assistEn: 'Musiala' }
  ],
  'usa': [
    { ar: 'بوليسيتش', en: 'Pulisic', assistAr: 'وياه', assistEn: 'Weah' },
    { ar: 'تيموثي وياه', en: 'Timothy Weah', assistAr: 'رينيه', assistEn: 'Reyna' }
  ],
  'mex': [
    { ar: 'سانتياغو خيمينيز', en: 'Santiago Gimenez', assistAr: 'تشافيز', assistEn: 'Chavez' },
    { ar: 'هيرفينغ لوزانو', en: 'Hirving Lozano', assistAr: 'ألفاريز', assistEn: 'Alvarez' }
  ],
  'default': [
    { ar: 'نجم الهجوم', en: 'Star Striker', assistAr: 'صانع اللعب', assistEn: 'Playmaker' },
    { ar: 'الجناح الطائر', en: 'Fast Winger', assistAr: 'لاعب الوسط', assistEn: 'Midfielder' },
    { ar: 'قلب الدفاع', en: 'Center Back', assistAr: 'الظهير الأيمن', assistEn: 'Right Back' }
  ]
};

function getStarPlayer(teamId) {
  const stars = TEAM_STARS[teamId] || TEAM_STARS['default'];
  return stars[Math.floor(Math.random() * stars.length)];
}

// ── Core sync function ────────────────────────────────────────────
async function syncNow() {
  const timestamp = new Date().toLocaleTimeString('ar-SA');
  console.log(`\n[${timestamp}] 🔄 جاري جلب البيانات من worldcup26.ir...`);

  try {
    // 0. Fetch existing matches to preserve and generate events
    const dbSnapshot = await get(ref(db, 'matches'));
    const existingMatches = dbSnapshot.exists() ? dbSnapshot.val() : {};

    // 1. Fetch all games from real API
    const res = await axios.get(`${API_BASE}/get/games`, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'application/json'
      },
      httpsAgent: new https.Agent({ rejectUnauthorized: false })
    });
    if (res.status !== 200) throw new Error(`API returned ${res.status}`);
    const games = res.data.games;

    // 2. Filter group stage only (or all)
    const targetGames = GROUP_STAGE_ONLY
      ? games.filter(g => g.type === 'group')
      : games;

    // 3. Transform API data to Firebase format
    const matchUpdates = {};
    let liveCount = 0, finishedCount = 0, upcomingCount = 0;

    for (const game of targetGames) {
      const matchId = `wc_${game.id}`;
      const status = mapStatus(game);
      const stadium = STADIUMS_AR[parseInt(game.stadium_id)] || {
        nameAr: 'ملعب كأس العالم', nameEn: 'World Cup Stadium',
        cityAr: 'أمريكا الشمالية', cityEn: 'North America'
      };
      const { date, time } = parseDate(game.local_date);

      const homeTeamObj = buildTeam(game.home_team_name_en, game.group);
      const awayTeamObj = buildTeam(game.away_team_name_en, game.group);
      const newHomeScore = status !== 'UPCOMING' ? parseInt(game.home_score) || 0 : null;
      const newAwayScore = status !== 'UPCOMING' ? parseInt(game.away_score) || 0 : null;

      // Preserve existing events and automatically generate missing goals
      const existingMatch = existingMatches[matchId] || {};
      let events = existingMatch.events ? [...existingMatch.events] : [];

      if (newHomeScore !== null && newAwayScore !== null) {
        const currentHomeGoals = events.filter(e => e.type === 'GOAL' && e.teamId === homeTeamObj.id).length;
        const currentAwayGoals = events.filter(e => e.type === 'GOAL' && e.teamId === awayTeamObj.id).length;

        for (let i = currentHomeGoals; i < newHomeScore; i++) {
          const star = getStarPlayer(homeTeamObj.id);
          events.push({
            id: `evt_${Date.now()}_h_${i}_${Math.random()}`,
            type: 'GOAL',
            minute: Math.floor(Math.random() * 90) + 1,
            teamId: homeTeamObj.id,
            playerNameAr: star.ar,
            playerNameEn: star.en,
            assistPlayerAr: star.assistAr,
            assistPlayerEn: star.assistEn,
          });
        }

        for (let i = currentAwayGoals; i < newAwayScore; i++) {
          const star = getStarPlayer(awayTeamObj.id);
          events.push({
            id: `evt_${Date.now()}_a_${i}_${Math.random()}`,
            type: 'GOAL',
            minute: Math.floor(Math.random() * 90) + 1,
            teamId: awayTeamObj.id,
            playerNameAr: star.ar,
            playerNameEn: star.en,
            assistPlayerAr: star.assistAr,
            assistPlayerEn: star.assistEn,
          });
        }
      }

      const match = {
        id: matchId,
        status,
        date,
        time,
        group: game.group,
        stadiumAr: stadium.nameAr,
        stadiumEn: stadium.nameEn,
        cityAr: stadium.cityAr,
        cityEn: stadium.cityEn,
        homeTeam: homeTeamObj,
        awayTeam: awayTeamObj,
        homeScore: newHomeScore,
        awayScore: newAwayScore,
        events: events,
      };

      // Add minute for live matches
      if (status === 'LIVE' && game.time_elapsed && game.time_elapsed !== 'notstarted') {
        const mins = parseInt(game.time_elapsed);
        if (!isNaN(mins)) match.minute = mins;
      }

      matchUpdates[match.id] = match;

      if (status === 'LIVE') liveCount++;
      else if (status === 'FINISHED') finishedCount++;
      else upcomingCount++;
    }

    // 4. Write all matches to Firebase atomically
    await set(ref(db, 'matches'), matchUpdates);

    console.log(`✅ تم التحديث: ${Object.keys(matchUpdates).length} مباراة`);
    console.log(`   🔴 مباشر: ${liveCount}  |  🏁 منتهية: ${finishedCount}  |  📅 قادمة: ${upcomingCount}`);

    // Show finished matches
    if (finishedCount > 0) {
      const finished = Object.values(matchUpdates).filter(m => m.status === 'FINISHED');
      finished.slice(0, 5).forEach(m => {
        console.log(`   ${m.homeTeam.flag} ${m.homeTeam.nameAr} ${m.homeScore}-${m.awayScore} ${m.awayTeam.nameAr} ${m.awayTeam.flag}`);
      });
      if (finished.length > 5) console.log(`   ... و ${finished.length - 5} مباريات أخرى`);
    }

    // Show live matches
    if (liveCount > 0) {
      const live = Object.values(matchUpdates).filter(m => m.status === 'LIVE');
      live.forEach(m => {
        console.log(`   🔴 LIVE: ${m.homeTeam.nameAr} ${m.homeScore}-${m.awayScore} ${m.awayTeam.nameAr} (${m.minute ?? '?'}')`);
      });
    }

    // Sync News automatically alongside matches
    await syncNews();

  } catch (err) {
    console.error(`❌ خطأ في الجلب: ${err.message}`);
  }
}

// ── Main ──────────────────────────────────────────────────────────
console.log('🚀 محرك التغذية الحية - Live Sync Engine');
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log(`📡 المصدر: ${API_BASE}`);
console.log(`🔥 Firebase: https://worldcup2026-live-default-rtdb.firebaseio.com`);
console.log(`⏱️  التحديث: كل ${SYNC_INTERVAL_MS / 1000} ثانية`);
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log('اضغط Ctrl+C لإيقاف المحرك\n');

// Run immediately then every 60s
await syncNow();
setInterval(syncNow, SYNC_INTERVAL_MS);
