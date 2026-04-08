// Simple clean US map outline
window.US_MAP_SVG = '<svg viewBox="0 0 960 600" style="width:100%;height:100%">' +
'<defs><linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%"><stop offset="0%" stop-color="#dbeafe"/><stop offset="100%" stop-color="#bfdbfe"/></linearGradient></defs>' +
'<rect width="960" height="600" fill="url(#bg)"/>' +
// Continental US outline (simplified but recognizable)
'<path fill="#f1f5f9" stroke="#64748b" stroke-width="2" d="' +
'M 120,120 L 190,100 L 230,105 L 230,175 L 190,190 L 140,195 L 100,175 L 90,140 Z ' + // WA
'M 90,175 L 140,195 L 170,285 L 100,330 L 60,285 L 70,220 Z ' + // OR/NV
'M 60,285 L 100,330 L 130,420 L 100,480 L 60,450 L 50,370 Z ' + // CA
'M 170,285 L 230,290 L 230,390 L 170,395 L 130,420 L 100,330 Z ' + // NV/AZ
'M 230,105 L 320,95 L 380,100 L 380,180 L 320,190 L 230,175 Z ' + // MT/ID
'M 230,175 L 320,190 L 380,180 L 380,280 L 320,290 L 230,290 Z ' + // WY/UT
'M 230,290 L 320,290 L 380,280 L 380,390 L 320,400 L 230,390 Z ' + // CO/NM
'M 380,100 L 500,95 L 500,175 L 380,180 Z ' + // ND/SD
'M 380,180 L 500,175 L 520,260 L 380,280 Z ' + // NE
'M 380,280 L 520,260 L 540,350 L 380,390 Z ' + // KS
'M 380,390 L 540,350 L 560,440 L 400,480 L 320,470 L 320,400 Z ' + // OK/TX
'M 320,470 L 400,480 L 450,570 L 350,560 L 300,520 Z ' + // TX
'M 500,95 L 580,100 L 600,170 L 500,175 Z ' + // MN
'M 500,175 L 600,170 L 620,250 L 520,260 Z ' + // IA
'M 520,260 L 620,250 L 640,340 L 540,350 Z ' + // MO
'M 540,350 L 640,340 L 650,420 L 560,440 Z ' + // AR
'M 560,440 L 650,420 L 660,490 L 580,520 L 500,510 L 450,570 L 400,480 Z ' + // LA/MS
'M 600,170 L 680,160 L 700,240 L 620,250 Z ' + // WI/IL
'M 620,250 L 700,240 L 720,320 L 640,340 Z ' + // IN/IL
'M 640,340 L 720,320 L 730,400 L 650,420 Z ' + // KY/TN
'M 650,420 L 730,400 L 740,480 L 660,490 Z ' + // AL/MS
'M 660,490 L 740,480 L 750,560 L 700,580 L 650,550 L 580,520 Z ' + // FL
'M 680,160 L 750,140 L 780,200 L 700,240 Z ' + // MI
'M 700,240 L 780,200 L 810,270 L 720,320 Z ' + // OH/IN
'M 720,320 L 810,270 L 830,350 L 730,400 Z ' + // WV/VA
'M 730,400 L 830,350 L 850,420 L 750,450 L 740,480 Z ' + // NC
'M 740,480 L 750,450 L 850,420 L 860,480 L 790,520 L 750,560 Z ' + // SC/GA
'M 780,200 L 850,170 L 880,240 L 810,270 Z ' + // PA
'M 810,270 L 880,240 L 900,310 L 830,350 Z ' + // MD/DE/NJ
'M 850,170 L 900,140 L 920,200 L 880,240 Z ' + // NY
'M 880,110 L 920,95 L 940,140 L 900,140 Z ' + // VT/NH/ME
'M 900,140 L 940,140 L 950,200 L 920,200 Z ' + // MA/CT/RI
'"/>' +
// City dots
'<g fill="#3b82f6" font-size="10" font-family="system-ui">' +
'<circle cx="160" cy="140" r="4"/><text x="165" y="135" fill="#475569">Seattle</text>' +
'<circle cx="85" cy="340" r="4"/><text x="90" y="345" fill="#475569">SF</text>' +
'<circle cx="100" cy="430" r="4"/><text x="105" y="435" fill="#475569">LA</text>' +
'<circle cx="300" cy="340" r="4"/><text x="305" y="345" fill="#475569">Denver</text>' +
'<circle cx="420" cy="490" r="4"/><text x="425" y="495" fill="#475569">Dallas</text>' +
'<circle cx="600" cy="220" r="4"/><text x="605" y="225" fill="#475569">Chicago</text>' +
'<circle cx="720" cy="450" r="4"/><text x="680" y="455" fill="#475569">Atlanta</text>' +
'<circle cx="870" cy="260" r="4"/><text x="830" y="255" fill="#475569">NYC</text>' +
'<circle cx="730" cy="550" r="4"/><text x="735" y="555" fill="#475569">Miami</text>' +
'</g>' +
'<g id="map-markers"></g>' +
'</svg>';
