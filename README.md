# Dashboard Comercial PAAS

Dashboard de indicadores comerciais da PAAS Poços Artesianos, conectado à API do RD Station CRM.

## Como funciona

- GitHub Actions roda automaticamente a cada 6 horas
- Busca todos os deals da API do RD Station CRM
- Gera o arquivo data/deals.json com os dados processados
- O dashboard carrega esse JSON quando alguém acessa

## Senha de acesso

Senha padrão: paas2026
