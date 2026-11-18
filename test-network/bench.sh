#!/usr/bin/env bash
# Uso (dentro de fabric-samples/test-network, com as variáveis do peer Org1 já exportadas):
#   ./bench.sh full 100     # 100 escritas com payload completo
#   ./bench.sh hash 100     # 100 escritas só com hash
#   BATCH_SIZE=100 ./bench.sh merkle 1000  # 1000 eventos ancorados em 10 transações
#   ./bench.sh verify 100   # 100 verificações (query, sem ordering)
#   ./bench.sh verify-merkle 1000 merkle-batches/<lote>.json <raiz-consultada>
# Observação: a latência inclui o overhead do CLI do peer (~centenas de ms). Serve para
# comparar as estratégias entre si; para números "limpos", usar Caliper ou fabric-gateway.
set -euo pipefail
now_ms() { local up _; read -r up _ < /proc/uptime; echo "${up/./}0"; }

MODE=${1:?full|hash|merkle|verify}; N=${2:-100}
BATCH_SIZE=${BATCH_SIZE:-100}
CC=evidence; CH=mychannel
ORD_CA="${PWD}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem"
ORG1="${PWD}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt"
ORG2="${PWD}/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt"
RUN=$(date +%s)
OUT="results_${MODE}_${RUN}.csv"
case $MODE in
  full|hash|verify) ;;
  merkle)
    if ! [[ "$N" =~ ^[1-9][0-9]*$ && "$BATCH_SIZE" =~ ^[1-9][0-9]*$ ]]; then
      echo "N e BATCH_SIZE devem ser inteiros positivos" >&2
      exit 1
    fi
    ;;
  verify-merkle)
    if ! [[ "$N" =~ ^[1-9][0-9]*$ && -f "${3:-}" && "${4:-}" =~ ^[a-f0-9]{64}$ ]]; then
      echo "Uso: ./bench.sh verify-merkle <iterações> <lote.json> <raiz-consultada>" >&2
      exit 1
    fi
    ;;
  *) echo "Modo inválido: $MODE (use full, hash, merkle, verify ou verify-merkle)" >&2; exit 1 ;;
esac
echo "i,latency_ms" > "$OUT"
MERKLE_TOOL="../../evidence-cc/merkle.js"

invoke() {
  peer chaincode invoke -o localhost:7050 --ordererTLSHostnameOverride orderer.example.com \
    --tls --cafile "$ORD_CA" -C $CH -n $CC \
    --peerAddresses localhost:7051 --tlsRootCertFiles "$ORG1" \
    --peerAddresses localhost:9051 --tlsRootCertFiles "$ORG2" \
    --waitForEvent -c "$1" >/dev/null 2>&1
}

START=$(now_ms)
if [[ "$MODE" == verify-merkle ]]; then
  SUMMARY=$(node "$MERKLE_TOOL" benchmark "$3" "$4" "$N")
  AVERAGE_US=$(printf '%s' "$SUMMARY" | jq -r .average_us)
  echo "1,$(echo "scale=6; $AVERAGE_US/1000" | bc)" >> "$OUT"
elif [[ "$MODE" == merkle ]]; then
  MERKLE_STORE=${MERKLE_STORE:-./merkle-batches}
  TMP_DIR=$(mktemp -d)
  trap 'rm -rf "$TMP_DIR"' EXIT
  mkdir -p "$MERKLE_STORE"
  batch_number=0
  OFFCHAIN_BYTES=0
  for ((first=1; first<=N; first+=BATCH_SIZE)); do
    batch_number=$((batch_number + 1))
    batch_count=$((N - first + 1))
    if (( batch_count > BATCH_SIZE )); then batch_count=$BATCH_SIZE; fi
    BATCH_ID="batch-${RUN}-${batch_number}"
    EVENTS_NDJSON="$TMP_DIR/events-${batch_number}.ndjson"
    EVENTS_JSON="$TMP_DIR/events-${batch_number}.json"
    BUNDLE="$MERKLE_STORE/${BATCH_ID}.json"
    : > "$EVENTS_NDJSON"
    WINDOW_START=$(date -u +%FT%TZ)
    for ((offset=0; offset<batch_count; offset++)); do
      i=$((first + offset))
      PAYLOAD=$(jq -nc --arg station "S$((i%8+1))" \
        --arg product "did:example:p$((i%20))" --argjson value "$RANDOM" \
        --arg opc_ts "$(date -u +%FT%TZ)" \
        '{station:$station,product:$product,value:$value,opc_ts:$opc_ts}')
      jq -nc --arg id "ev-${RUN}-${i}" --argjson payload "$PAYLOAD" \
        '{id:$id,payload:$payload}' >> "$EVENTS_NDJSON"
    done
    WINDOW_END=$(date -u +%FT%TZ)
    jq -s . "$EVENTS_NDJSON" > "$EVENTS_JSON"
    SUMMARY=$(node "$MERKLE_TOOL" build "$BATCH_ID" "$EVENTS_JSON" "$BUNDLE" \
      "$WINDOW_START" "$WINDOW_END")
    ROOT=$(printf '%s' "$SUMMARY" | jq -r .root)
    OFFCHAIN_BYTES=$((OFFCHAIN_BYTES + $(wc -c < "$BUNDLE")))
    T0=$(now_ms)
    invoke "$(jq -nc --arg id "$BATCH_ID" --arg root "$ROOT" \
      --argjson count "$batch_count" --arg start "$WINDOW_START" --arg end "$WINDOW_END" \
      '{function:"registerBatchRoot",Args:[$id,$root,($count|tostring),({start:$start,end:$end}|tojson)]}')"
    T1=$(now_ms)
    echo "$batch_number,$(( T1-T0 ))" >> "$OUT"
  done
else
  for i in $(seq 1 "$N"); do
    ID="ev-${RUN}-${i}"
    PAYLOAD="{\"station\":\"S$((i%8+1))\",\"product\":\"did:example:p$((i%20))\",\"value\":$RANDOM,\"opc_ts\":\"$(date -u +%FT%TZ)\"}"
    HASH=$(printf '%s' "$PAYLOAD" | sha256sum | cut -d' ' -f1)
    PDID="did:example:p$((i%20))"
    T0=$(now_ms)
    case $MODE in
      full)   invoke "$(jq -nc --arg a "$ID" --arg b "$PDID" --arg c "$PAYLOAD" '{function:"registerEvidenceFull",Args:[$a,$b,$c]}')" ;;
      hash)   invoke "$(jq -nc --arg a "$ID" --arg b "$PDID" --arg c "$HASH" '{function:"registerEvidenceHash",Args:[$a,$b,$c,"{\"station\":\"S1\"}"]}')" ;;
      verify) # verifica uma evidência pré-existente informada em $VERIFY_ID / $VERIFY_HASH
              peer chaincode query -C $CH -n $CC \
                -c "$(jq -nc --arg a "${VERIFY_ID:?}" --arg b "${VERIFY_HASH:?}" '{function:"verifyEvidence",Args:[$a,$b]}')" >/dev/null ;;
    esac
    T1=$(now_ms)
    echo "$i,$(( T1-T0 ))" >> "$OUT"
  done
fi
END=$(now_ms)
if [[ "$MODE" == merkle ]]; then
  ONCHAIN_BYTES=0
  for ((batch=1; batch<=batch_number; batch++)); do
    BATCH_ID="batch-${RUN}-${batch}"
    peer chaincode query -C "$CH" -n "$CC" \
      -c "$(jq -nc --arg id "$BATCH_ID" '{function:"getBatchRoot",Args:[$id]}')" \
      | jq -c . > "$TMP_DIR/state-${batch}.json"
    ONCHAIN_BYTES=$((ONCHAIN_BYTES + $(wc -c < "$TMP_DIR/state-${batch}.json")))
  done
fi

SECS=$(echo "scale=3; ($END-$START)/1000" | bc)
if [[ "$MODE" == merkle ]]; then
  TX_COUNT=$(((N + BATCH_SIZE - 1) / BATCH_SIZE))
  echo "Modo=merkle eventos=$N lotes=$TX_COUNT tamanho_lote=$BATCH_SIZE tempo_total=${SECS}s throughput=$(echo "scale=2; $N/$SECS" | bc) eventos/s transacoes=$(echo "scale=2; $TX_COUNT/$SECS" | bc) tx/s"
  echo "Armazenamento: off-chain=${OFFCHAIN_BYTES} bytes; valores do ledger=${ONCHAIN_BYTES} bytes"
  echo "Lotes e provas off-chain: $MERKLE_STORE"
elif [[ "$MODE" == verify-merkle ]]; then
  echo "$SUMMARY"
else
  echo "Modo=$MODE N=$N tempo_total=${SECS}s throughput=$(echo "scale=2; $N/$SECS" | bc) tx/s"
fi
awk -F, 'NR>1{s+=$2;a[NR]=$2} END{print "latência média="s/(NR-1)" ms"}' "$OUT"
sort -t, -k2 -n "$OUT" | awk -F, 'NR>1{v[NR-1]=$2} END{print "p95="v[int((NR-1)*0.95)]" ms"}'
echo "CSV: $OUT"