
## **Nome:** bcb_sgs_daily_trusted
### **Fonte:** BCB SGS

**Séries:** 432 (SELIC), 433 (IPCA)
**Granularidade:** diária
**Chave:** (series_id, ref_date)
**Schema:** lista colunas + tipos

**Transformações:**
- parse data (dd/mm/yyyy) → ref_date
- parse valor string → numérico
- dedup por (series_id, ref_date)
- adiciona auditoria (raw_file, raw_hash, ingestion_ts_utc)
- Qualidade: sem nulos em series_id/ref_date; sem duplicata na chave

record_hash: “hash determinístico do conteúdo do registro (serie_id, data, valor normalizado) — usado para idempotência e detecção de mudanças.”
