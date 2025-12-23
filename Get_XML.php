<?php
// Get_XML.php
// Snelle, robuuste versie met automatische JSON-check voor foute bruggen.
// Zorg dat map 'get' schrijfbaar is door de webserver voor log & output.

require_once __DIR__ . '/db_helpers.php';

date_default_timezone_set("UTC");

// ---------- Config ----------
$jsonInputFile  = __DIR__ . '/get/bruggen.json';
$jsonOutputFile = __DIR__ . '/get/bruggen_open.json';
$logBadFile     = __DIR__ . '/get/foute_bruggen.log';
$ndwUrl         = "http://opendata.ndw.nu/brugopeningen.xml.gz";

$dbConfig = load_db_config();

// ---------- Helpers ----------
function safe_get_string($var) {
    return isset($var) ? (string)$var : '';
}

/**
 * Zorgt ervoor dat ndwID zowel als string als array kan worden opgegeven.
 * Lege waarden worden gefilterd en de uitkomst is altijd een array van strings.
 */
function normalize_ndw_ids($value) {
    $ids = [];

    if (is_array($value)) {
        $ids = $value;
    } elseif ($value !== null) {
        $ids = [$value];
    }

    $ids = array_map(function ($val) {
        return trim((string)$val);
    }, $ids);

    $ids = array_filter($ids, function ($val) {
        return $val !== '';
    });

    return array_values(array_unique($ids));
}

function parse_iso_datetime($value) {
    if (!is_string($value) || trim($value) === '') {
        return null;
    }

    try {
        return new DateTime($value);
    } catch (Exception $e) {
        return null;
    }
}

function derive_bridge_status(array $candidate, DateTime $now) {
    $start       = $candidate['start'];
    $end         = $candidate['end'];
    $validity    = $candidate['validityStatus'];
    $probability = $candidate['probability'];
    $operator    = $candidate['operatorStatus'];

    $status      = 'dicht';
    $isOpen      = false;
    $isPlanned   = false;
    $statusMomentRaw = $candidate['startRaw'] ?: '';

    $windowActive = $start && $start <= $now && (!$end || $end >= $now);

    if ($windowActive && ($validity === 'active' || $probability === 'certain' || $operator === 'beingCarriedOut')) {
        $status = 'open';
        $isOpen = true;
    } elseif ($start && $start > $now) {
        $status = 'gepland';
        $isPlanned = true;
    } elseif ($validity === 'planned' || $probability === 'probable' || $operator === 'approved') {
        $status = 'gepland';
        $isPlanned = true;
    } else {
        $statusMomentRaw = $candidate['endRaw'] ?: $statusMomentRaw;
    }

    $distance = $start ? abs($now->getTimestamp() - $start->getTimestamp()) : PHP_INT_MAX;
    if ($isOpen && $start && $start <= $now && (!$end || $end >= $now)) {
        $distance = 0;
    }

    return [
        'status' => $status,
        'open' => $isOpen,
        'planning' => $isPlanned,
        'statusMomentRaw' => $statusMomentRaw ?: $now->format('Y-m-d\TH:i:s.v\Z'),
        'distance' => $distance
    ];
}

function status_priority($status) {
    switch ($status) {
        case 'open':
            return 3;
        case 'gepland':
            return 2;
        default:
            return 1;
    }
}

// ---------- Inlezen bruggen.json ----------
if (!file_exists($jsonInputFile)) {
    die("Input bestand niet gevonden: $jsonInputFile\n");
}

$json_data = file_get_contents($jsonInputFile);
$bruggen_raw = json_decode($json_data, true);

if (!is_array($bruggen_raw)) {
    // Log en stop
    file_put_contents($logBadFile, date('c') . " - Ongeldige JSON in bruggen.json\n", FILE_APPEND);
    die("bruggen.json kon niet worden geparsed als JSON.\n");
}

// ---------- Valideer en normaliseer JSON (en log foute items) ----------
$bruggen = [];
foreach ($bruggen_raw as $index => $item) {

    // Normalize keys: sommige bestanden hebben Lat/Lon/Naam/ISRS
    // Maak een tolk zodat de rest van de code met id/latitude/longitude/naam kan werken.
    if (isset($item['ISRS'])) {
        $item['id'] = $item['ISRS'];
    }
    if (isset($item['Lat'])) {
        $item['latitude'] = $item['Lat'];
    }
    if (isset($item['Lon'])) {
        $item['longitude'] = $item['Lon'];
    }
    if (isset($item['Naam'])) {
        $item['naam'] = $item['Naam'];
    }

    // Vereiste velden (id, latitude, longitude, naam)
    $ok = true;
    $missing = [];

    if (!isset($item['id']) || $item['id'] === '') {
        $ok = false;
        $missing[] = 'id';
    }
    if (!isset($item['latitude']) || $item['latitude'] === '' || !is_numeric($item['latitude'])) {
        $ok = false;
        $missing[] = 'latitude';
    }
    if (!isset($item['longitude']) || $item['longitude'] === '' || !is_numeric($item['longitude'])) {
        $ok = false;
        $missing[] = 'longitude';
    }
    if (!isset($item['naam']) || $item['naam'] === '') {
        $ok = false;
        $missing[] = 'naam';
    }

    if (!$ok) {
        $logLine = date('c') . " - Ongeldige brug op index $index. Missende/invalid keys: " . implode(',', $missing) . "\n";
        $logLine .= print_r($item, true) . "\n---\n";
        file_put_contents($logBadFile, $logLine, FILE_APPEND);
        continue; // skip deze brug
    }

    // Ondersteun zowel string als array voor ndwID's
    $item['ndwIDs'] = normalize_ndw_ids($item['ndwID'] ?? []);

    // Alles OK: voeg toe aan genormaliseerde lijst
    $bruggen[] = $item;
}

if (count($bruggen) === 0) {
    die("Geen geldige bruggen gevonden na validatie. Kijk in $logBadFile voor details.\n");
}

// ---------- Database init ----------
$historyTable = sanitize_table_name($dbConfig['table']);
$pdo = null;

try {
    $pdo = init_db($dbConfig, $historyTable);
} catch (Throwable $e) {
    // Geen database beschikbaar: log dit en ga verder zodat de hoofdscript niet crasht
    file_put_contents(
        $logBadFile,
        date('c') . " - Database niet beschikbaar: " . $e->getMessage() . "\n",
        FILE_APPEND
    );
}

// ---------- NDW XML ophalen en parsen ----------
$xml_gz_content = @file_get_contents($ndwUrl);
if ($xml_gz_content === false) {
    file_put_contents($logBadFile, date('c') . " - Fout bij ophalen NDW URL: $ndwUrl\n", FILE_APPEND);
    die("Kon NDW XML niet ophalen.\n");
}

$xml_content = @gzdecode($xml_gz_content);
if ($xml_content === false) {
    file_put_contents($logBadFile, date('c') . " - Fout bij gzdecode van NDW content\n", FILE_APPEND);
    die("Fout bij gzdecode.\n");
}

$rcXML = @simplexml_load_string($xml_content);
if ($rcXML === false) {
    file_put_contents($logBadFile, date('c') . " - Fout bij simplexml_load_string op NDW content\n", FILE_APPEND);
    die("Fout bij parsen NDW XML.\n");
}

// Navigeer naar situations (defensie tegen ontbrekende nodes)
$envelope = $rcXML->children('http://schemas.xmlsoap.org/soap/envelope/');
$body     = $envelope->Body ?? null;
$datex    = $body ? $body->children('http://datex2.eu/schema/2/2_0') : null;

if (!$datex || !isset($datex->d2LogicalModel->payloadPublication->situation)) {
    file_put_contents($logBadFile, date('c') . " - NDW XML heeft geen situation nodes\n", FILE_APPEND);
    // We gaan door met lege situaties
    $arraySituation = [];
} else {
    $arraySituation = $datex->d2LogicalModel->payloadPublication->situation;
}

// ---------- Indexeer situaties op afgeronde coördinaten (lat_ lon) ----------
// Hiermee voorkomen we n x m loops. We bewaren enkel situaties waarvan overallStartTime +2h > now
$situationMap = [];
$now = new DateTime();

foreach ($arraySituation as $situation) {

    // Beveiliging: check of nodes bestaan
    $loc = $situation->situationRecord->groupOfLocations->locationForDisplay ?? null;
    $validityNode = $situation->situationRecord->validity->validityTimeSpecification ?? null;

    if (!$loc || !$validityNode) continue;

    $latRaw = safe_get_string($loc->latitude);
    $lonRaw = safe_get_string($loc->longitude);
    $startRaw = safe_get_string($validityNode->overallStartTime);
    $endRaw = safe_get_string($validityNode->overallEndTime);
    $validityStatus = safe_get_string($situation->situationRecord->validity->validityStatus);
    $probability = safe_get_string($situation->situationRecord->probabilityOfOccurrence);
    $operatorStatus = safe_get_string($situation->situationRecord->operatorActionStatus);

    if ($latRaw === '' || $lonRaw === '' || $startRaw === '') continue;

    if (!is_numeric((string)$latRaw) || !is_numeric((string)$lonRaw)) continue;

    $lat = round((float)$latRaw, 5);
    $lon = round((float)$lonRaw, 5);
    $key = $lat . '_' . $lon;

    $startDt = parse_iso_datetime($startRaw);
    if (!$startDt) continue;

    $endDt = parse_iso_datetime($endRaw);

    $toekomst = (clone $startDt)->modify("+2 hours");
    if ($toekomst > $now) {
        // bewaar situatie
        if (!isset($situationMap[$key])) $situationMap[$key] = [];
        $situationMap[$key][] = [
            'node' => $situation,
            'start' => $startDt,
            'end' => $endDt,
            'startRaw' => $startRaw,
            'endRaw' => $endRaw,
            'validityStatus' => $validityStatus,
            'probability' => $probability,
            'operatorStatus' => $operatorStatus
        ];
    }
}

// ---------- Verwerk elke brug: 1 lookup op key, kies dichtsbijzijnde starttime ----------
$dataArray = [];

foreach ($bruggen as $brug) {

    $lat = round((float)$brug['latitude'], 5);
    $lon = round((float)$brug['longitude'], 5);
    $key = $lat . '_' . $lon;

    $found = null;
    $GetDatumEinde = '';
    $planning = false;
    $openFlag = false;
    $statusMomentRaw = '';

    if (isset($situationMap[$key])) {
        // kies situatie waarvan overallStartTime het dichtst bij nu ligt
        foreach ($situationMap[$key] as $candidate) {
            $candidate['derived'] = derive_bridge_status($candidate, $now);

            if ($found === null) {
                $found = $candidate;
                continue;
            }

            $currentPriority = status_priority($found['derived']['status']);
            $candidatePriority = status_priority($candidate['derived']['status']);

            if ($candidatePriority > $currentPriority) {
                $found = $candidate;
                continue;
            }

            if ($candidatePriority === $currentPriority) {
                if ($candidate['derived']['distance'] < $found['derived']['distance']) {
                    $found = $candidate;
                }
            }
        }
    }

    // Vul het output-object (zelfde velden als jouw oude script, maar robuust)
    if ($found) {
        $SituationCurrent   = (string)($found['node']->situationRecord->operatorActionStatus ?? '');
        $SituationVoorspeld = (string)($found['node']->situationRecord->probabilityOfOccurrence ?? '');
        // attributes() kan ontbrekend zijn of korter zijn; bescherm tegen notices
        $attributes = $found['node']->situationRecord->attributes();
        $ndwVersion = isset($attributes[1]) ? (string)$attributes[1] : "0";
        $GetDatumStart = $found['startRaw'] ?? (string)($found['node']->situationRecord->validity->validityTimeSpecification->overallStartTime ?? '');
        $derived = $found['derived'];
        $status = $derived['status'];
        $planning = $derived['planning'];
        $openFlag = $derived['open'];
        $statusMomentRaw = $derived['statusMomentRaw'];
    } else {
        $SituationCurrent   = "certain";
        $SituationVoorspeld = "beingTerminated";
        $ndwVersion         = "0";
        $status             = "dicht";
        $GetDatumStart      = (new DateTime())->format('Y-m-d\TH:i:s.v\Z');
        $planning           = false;
        $openFlag           = false;
        $statusMomentRaw    = $GetDatumStart;
    }

    $GetDatumEinde = $found['endRaw'] ?? $GetDatumEinde;

    $statusMoment = $statusMomentRaw ?: $GetDatumStart ?: (new DateTime())->format('Y-m-d\TH:i:s.v\Z');
    log_status($pdo, $brug['id'], $status, $statusMoment, $historyTable);

    // Bouw output voor deze brug
    $dataArray[] = [
        'Id' => $brug['id'],
        'Data' => [
            'latitude' => (float)$brug['latitude'],
            'longitude' => (float)$brug['longitude'],
            'SituationCurrent' => $SituationCurrent,
            'SituationVoorspeld' => $SituationVoorspeld,
            'ndwVersion' => $ndwVersion,
            'GetDatumStart' => $GetDatumStart,
            'GetDatumEinde' => $GetDatumEinde,
            'validityStatus' => $found['validityStatus'] ?? '',
            'operatorActionStatus' => $found['operatorStatus'] ?? '',
            'planning' => $planning,
            'Naam' => $brug['naam'],
            'Provincie' => $brug['provincie'],
            'Stad' => $brug['stad'],
            'ndwIDs' => $brug['ndwIDs'],
            'status' => $status,
            'open' => $openFlag
        ]
    ];
}

// ---------- Sla output op ----------
file_put_contents($jsonOutputFile, json_encode($dataArray, JSON_PRETTY_PRINT));

// ---------- Einde ----------
echo "Verwerking klaar. Output: $jsonOutputFile\n";
echo "Foute items (indien aanwezig) in: $logBadFile\n";
?>
