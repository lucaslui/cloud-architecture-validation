import java.util.concurrent.ThreadLocalRandom
import org.apache.commons.io.FileUtils
import org.apache.jmeter.engine.util.CompoundVariable
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

// ===== Config obrigatória =====
def base = vars.get('SAMPLES_DIR')
if (!base) throw new IllegalStateException('SAMPLES_DIR não definido (use -JSAMPLES_DIR=/abs/path/para/samples)')

// ===== Lista SOMENTE Telemetry =====
def entries = [
  [path:'appliances/dish-washer-telemetry.json',  deviceType:'Appliance', category:'Dishwasher'],
  [path:'appliances/refrigerator-telemetry.json', deviceType:'Appliance', category:'Refrigerator'],
  [path:'appliances/television-telemetry.json',   deviceType:'Appliance', category:'Television'],
  [path:'appliances/washing-machine-telemetry.json', deviceType:'Appliance', category:'WashingMachine'],
  [path:'battery-bank-telemetry.json', deviceType:'BatteryBank'],
  [path:'env-sensor-telemetry.json', deviceType:'EnvironmentalSensor'],
  [path:'ev-telemetry.json', deviceType:'ElectricVehicle'],
  [path:'hvac-telemetry.json', deviceType:'HVAC'],
  [path:'res-telemetry.json', deviceType:'RES'],
  [path:'smart-meter-telemetry.json', deviceType:'SmartMeter'],
]
if (entries.isEmpty()) throw new IllegalStateException('Nenhum payload Telemetry encontrado.')

// ===== N da casa (antes da avaliação p/ ${HOME_NUMBER}) =====
int houseNumber = ctx.getThreadNum() + 1
String HOUSE_NUMBER = String.valueOf(houseNumber)

// ===== TIMESTAMP (UTC) antes da avaliação p/ ${TIMESTAMP} =====
String nowUtc = java.time.ZonedDateTime
  .now(java.time.ZoneId.systemDefault())
  .withZoneSameInstant(java.time.ZoneId.of("UTC"))
  .format(java.time.format.DateTimeFormatter.ISO_INSTANT)

// ===== Escolha simples (aleatória uniforme) =====
def pick = entries[ ThreadLocalRandom.current().nextInt(entries.size()) ]

// ===== Leitura do arquivo =====
def file = new File(base, pick.path)
if (!file.exists()) throw new IllegalStateException("Arquivo não encontrado: ${file.absolutePath}")
def raw = FileUtils.readFileToString(file, 'UTF-8').trim()
if (raw.isEmpty()) throw new IllegalStateException("Arquivo vazio: ${file.absolutePath}")

// ===== Suporte offline a __chooseRandom(...): ',' ou '|' ; ignora nome de var no fim =====
raw = raw.replaceAll(/\$\{__chooseRandom\(([^}]*)\)\}/) { full, inside ->
  def parts = inside.split(/[|,]/).collect{ it.trim() }.findAll{ it }
  if (parts.size() > 1 && parts[-1] ==~ /^[A-Za-z_][A-Za-z0-9_]*$/) { parts = parts[0..-2] }
  def opts = parts.collect{ it.replaceAll(/^['"]|['"]$/, '') }
  if (opts.isEmpty()) throw new IllegalStateException("__chooseRandom sem opções em ${file.name}")
  opts[ ThreadLocalRandom.current().nextInt(opts.size()) ]
}

// ===== Substituição manual das variáveis críticas =====
raw = raw.replace('${HOME_NUMBER}', HOUSE_NUMBER).replace('${TIMESTAMP}', nowUtc)

if (raw.contains('${HOME_NUMBER}') || raw.contains('${TIMESTAMP}')) {
  throw new IllegalStateException("Placeholders obrigatórios remanescentes em ${file.name}")
}

// ===== Avalia só o que falta (ex.: __Random) =====
def evaluated = new CompoundVariable(raw).execute()
if (evaluated == null || evaluated.trim().isEmpty())
  throw new IllegalStateException("Após avaliar funções, conteúdo ficou vazio: ${file.name}")

// Falha se restarem placeholders ${...}
def leftovers = (evaluated =~ /\$\{[^}]+\}/).collect{ it[0] }.unique()
if (!leftovers.isEmpty())
  throw new IllegalStateException("Placeholders não resolvidos ${leftovers} no arquivo: ${file.name}")

// ===== Parse estrito =====
def parsed
try {
  parsed = new JsonSlurper().parseText(evaluated)
} catch (Throwable ex) {
  throw new IllegalStateException("JSON inválido após avaliação: ${file.name}. Trecho: " + evaluated.take(160), ex)
}
if (!(parsed instanceof Map)) throw new IllegalStateException("JSON raiz não é objeto: ${file.name}")

// ===== Timestamp UTC determinístico (sobrepõe o que veio do arquivo) =====
parsed.timestamp = nowUtc

// ===== Converte strings numéricas do payload (somente Telemetry) =====
if (parsed.eventType != 'Telemetry')
  throw new IllegalStateException("Somente Telemetry aqui; veio eventType=${parsed.eventType} em ${file.name}")
if (!(parsed.payload instanceof Map))
  throw new IllegalStateException("payload não é objeto em ${file.name}")

parsed.payload = parsed.payload.collectEntries { k, v ->
  if ((v instanceof String) && (v ==~ /-?\d+(\.\d+)?/)) [(k): new BigDecimal(v)] else [(k): v]
}

// ===== Serializa minificado e entrega =====
vars.put('BODY', JsonOutput.toJson(parsed))
