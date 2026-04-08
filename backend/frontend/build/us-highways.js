// US Map with Major Interstate Highways
// Simplified but recognizable paths for major interstates

window.US_HIGHWAYS_SVG = `
<svg viewBox="0 0 960 600" xmlns="http://www.w3.org/2000/svg">
  <!-- Ocean background -->
  <rect width="960" height="600" fill="#e0f2fe"/>
  
  <!-- State outlines (simplified continental US) -->
  <g id="states" fill="#f1f5f9" stroke="#94a3b8" stroke-width="1">
    <!-- West Coast -->
    <path d="M50,120 L80,80 L120,70 L140,100 L130,180 L100,280 L80,380 L100,450 L60,480 L40,400 L30,300 L40,200 Z"/>
    <!-- Pacific Northwest -->
    <path d="M80,80 L180,60 L200,100 L180,140 L140,100 Z"/>
    <!-- Mountain West -->
    <path d="M140,100 L200,100 L280,90 L300,140 L280,240 L240,340 L200,380 L160,340 L130,280 L130,180 Z"/>
    <!-- Southwest -->
    <path d="M130,280 L200,380 L280,420 L220,500 L140,480 L100,450 L80,380 L100,280 Z"/>
    <!-- Mountain States -->
    <path d="M200,100 L340,80 L360,120 L340,200 L300,280 L280,240 L300,140 Z"/>
    <!-- Plains North -->
    <path d="M340,80 L480,70 L500,100 L480,180 L420,200 L360,180 L360,120 Z"/>
    <!-- Plains Central -->
    <path d="M300,280 L420,260 L480,300 L460,380 L380,400 L300,360 L280,300 Z"/>
    <!-- Texas -->
    <path d="M280,420 L380,400 L460,440 L480,520 L400,560 L320,540 L260,500 L220,500 Z"/>
    <!-- Midwest -->
    <path d="M480,180 L580,160 L620,200 L600,280 L540,320 L480,300 L420,260 L420,200 Z"/>
    <!-- Great Lakes -->
    <path d="M580,160 L680,140 L720,180 L700,240 L640,260 L600,240 L600,200 Z"/>
    <!-- Ohio Valley -->
    <path d="M600,280 L700,260 L740,320 L700,380 L620,380 L560,340 L540,320 Z"/>
    <!-- Southeast -->
    <path d="M560,340 L620,380 L680,420 L720,480 L680,520 L600,500 L540,460 L500,400 L540,360 Z"/>
    <!-- Deep South -->
    <path d="M460,440 L540,460 L600,500 L560,540 L480,560 L480,520 Z"/>
    <!-- Florida -->
    <path d="M680,420 L740,420 L780,480 L760,560 L720,580 L680,540 L680,520 L720,480 Z"/>
    <!-- Mid-Atlantic -->
    <path d="M700,260 L780,240 L820,280 L800,340 L740,360 L700,340 L700,280 Z"/>
    <!-- Northeast -->
    <path d="M720,180 L800,160 L860,140 L880,180 L840,220 L780,240 L740,220 L720,200 Z"/>
    <!-- New England -->
    <path d="M840,100 L880,80 L920,100 L900,160 L860,180 L840,140 Z"/>
  </g>
  
  <!-- Major Interstate Highways -->
  <g id="highways" fill="none" stroke="#dc2626" stroke-width="2" opacity="0.7">
    <!-- I-10: LA to Jacksonville -->
    <path d="M80,420 L220,480 L380,460 L480,480 L600,460 L720,440" stroke-dasharray="none"/>
    <!-- I-20: West Texas to Atlanta -->
    <path d="M280,440 L380,420 L480,400 L600,400 L680,400"/>
    <!-- I-40: Barstow to Wilmington NC -->
    <path d="M100,360 L200,340 L300,320 L400,320 L500,320 L600,340 L700,340 L780,320"/>
    <!-- I-70: Utah to Baltimore -->
    <path d="M180,280 L280,260 L380,260 L480,260 L580,260 L680,280 L780,280"/>
    <!-- I-80: SF to NYC -->
    <path d="M60,300 L140,260 L240,220 L340,200 L440,200 L540,200 L640,200 L740,220 L820,240"/>
    <!-- I-90: Seattle to Boston -->
    <path d="M100,140 L200,120 L300,120 L400,140 L500,160 L600,160 L700,180 L800,180 L880,160"/>
    <!-- I-95: Miami to Maine -->
    <path d="M760,560 L740,480 L720,420 L740,360 L780,300 L820,260 L840,220 L860,180 L880,120"/>
    <!-- I-5: Seattle to San Diego -->
    <path d="M100,100 L80,200 L70,300 L80,400 L100,460"/>
    <!-- I-15: Montana to San Diego -->
    <path d="M200,80 L180,160 L160,260 L140,360 L120,440"/>
    <!-- I-25: Wyoming to El Paso -->
    <path d="M280,100 L280,200 L280,300 L280,400 L260,480"/>
    <!-- I-35: Duluth to Laredo -->
    <path d="M460,100 L460,200 L440,300 L420,400 L400,500"/>
    <!-- I-55: Chicago to New Orleans -->
    <path d="M560,200 L540,300 L520,400 L500,480"/>
    <!-- I-65: Chicago to Mobile -->
    <path d="M600,200 L620,300 L640,400 L620,480"/>
    <!-- I-75: Michigan to Miami -->
    <path d="M680,140 L680,240 L680,340 L700,420 L740,520"/>
    <!-- I-85: Montgomery to Virginia -->
    <path d="M660,420 L700,380 L740,340 L780,300"/>
  </g>
  
  <!-- Highway Labels -->
  <g id="highway-labels" font-size="8" fill="#991b1b" font-family="Arial" font-weight="bold">
    <text x="400" y="475">I-10</text>
    <text x="500" y="395">I-20</text>
    <text x="450" y="315">I-40</text>
    <text x="500" y="255">I-70</text>
    <text x="450" y="195">I-80</text>
    <text x="500" y="155">I-90</text>
    <text x="800" y="400">I-95</text>
    <text x="60" y="250">I-5</text>
    <text x="150" y="300">I-15</text>
    <text x="290" y="250">I-25</text>
    <text x="430" y="350">I-35</text>
    <text x="510" y="350">I-55</text>
    <text x="630" y="350">I-65</text>
    <text x="660" y="290">I-75</text>
  </g>
  
  <!-- City markers -->
  <g id="cities" font-size="9" fill="#1e40af" font-family="Arial">
    <circle cx="100" cy="140" r="4" fill="#3b82f6"/><text x="105" y="135">Seattle</text>
    <circle cx="80" cy="380" r="4" fill="#3b82f6"/><text x="60" y="395">LA</text>
    <circle cx="280" cy="480" r="4" fill="#3b82f6"/><text x="285" y="495">Dallas</text>
    <circle cx="560" cy="200" r="4" fill="#3b82f6"/><text x="565" y="195">Chicago</text>
    <circle cx="680" cy="420" r="4" fill="#3b82f6"/><text x="685" y="415">Atlanta</text>
    <circle cx="820" cy="260" r="4" fill="#3b82f6"/><text x="785" y="255">NYC</text>
    <circle cx="740" cy="540" r="4" fill="#3b82f6"/><text x="745" y="555">Miami</text>
    <circle cx="180" cy="340" r="4" fill="#3b82f6"/><text x="185" y="355">Phoenix</text>
    <circle cx="340" cy="300" r="4" fill="#3b82f6"/><text x="345" y="315">Denver</text>
    <circle cx="460" cy="280" r="4" fill="#3b82f6"/><text x="430" y="295">KC</text>
  </g>
  
  <!-- Markers layer - trucks and service centers go here -->
  <g id="routes-layer"></g>
  <g id="map-markers"></g>
</svg>
`;

