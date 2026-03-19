# Deploy Guide - Analytics Chatbot

## Pre-requisitos

- **Docker** >= 20.10
- **Docker Compose** >= 2.0
- **API Key:**
  - Google Gemini API Key (obrigatoria)
  - Supabase (opcional, para logging)

## Arquitetura

A imagem Docker e **autossuficiente**:
- Dataset embutido durante o build
- Sem necessidade de upload manual de arquivos
- Configuracao apenas via variavel de ambiente `GEMINI_API_KEY`

## Deploy Local (Desenvolvimento)

### 1. Clonar e Configurar

```bash
# Clonar repositorio
git clone https://github.com/target-solucoes/analytics-chatbot.git
cd analytics-chatbot

# Criar arquivo de ambiente
cp .env.example .env
```

### 2. Configurar Variavel de Ambiente

Edite o arquivo `.env` com sua chave de API:

```bash
GEMINI_API_KEY=your-gemini-api-key-here
```

### 3. Build e Execucao

```bash
# Build e iniciar container
docker-compose up --build

# Ou em modo detached (background)
docker-compose up --build -d
```

### 4. Acessar Aplicacao

Abra o navegador em: http://localhost:8501

## Deploy em Producao (GHCR)

### 1. Autenticacao no Registry

```bash
# Criar token em: GitHub Settings > Developer settings > Personal access tokens
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin
```

### 2. Pull da Imagem

```bash
docker pull ghcr.io/target-solucoes/analytics-chatbot:latest
```

### 3. Executar Container

```bash
docker run -d \
  --name analytics-chatbot \
  -p 8501:8501 \
  -e GEMINI_API_KEY=your-gemini-api-key \
  -v ./logs:/app/logs \
  -v ./data/output:/app/data/output \
  --restart unless-stopped \
  ghcr.io/target-solucoes/analytics-chatbot:latest
```

### 4. Ou usar Docker Compose

Crie um `docker-compose.prod.yml`:

```yaml
services:
  analytics-chatbot:
    image: ghcr.io/target-solucoes/analytics-chatbot:latest
    container_name: analytics-chatbot
    ports:
      - "8501:8501"
    environment:
      - GEMINI_API_KEY=${GEMINI_API_KEY}
    volumes:
      - ./logs:/app/logs
      - ./data/output:/app/data/output
    restart: unless-stopped
```

Execute com:

```bash
GEMINI_API_KEY=your-key docker-compose -f docker-compose.prod.yml up -d
```

## Comandos de Manutencao

### Verificar Status

```bash
# Status do container
docker-compose ps

# Health check
curl http://localhost:8501/_stcore/health
```

### Logs

```bash
# Ver logs em tempo real
docker-compose logs -f analytics-chatbot

# Ultimas 100 linhas
docker-compose logs --tail=100 analytics-chatbot
```

### Reiniciar

```bash
# Restart simples
docker-compose restart

# Restart com rebuild
docker-compose down && docker-compose up --build -d
```

### Atualizar para Nova Versao

```bash
# Pull nova imagem
docker pull ghcr.io/target-solucoes/analytics-chatbot:latest

# Recriar container
docker-compose down
docker-compose up -d
```

### Limpar Recursos

```bash
# Parar e remover container
docker-compose down

# Remover imagens nao utilizadas
docker image prune -f

# Remover tudo (cuidado!)
docker-compose down --rmi all --volumes
```

## Validacao do Deploy

### 1. Health Check

```bash
# Deve retornar "ok"
curl -f http://localhost:8501/_stcore/health
```

### 2. Verificar Logs

```bash
# Verificar se nao ha erros de startup
docker-compose logs analytics-chatbot | grep -i error
```

### 3. Teste Funcional

1. Acesse http://localhost:8501
2. Faca login (se autenticacao habilitada)
3. Envie uma query de teste: "top 5 clientes por vendas"
4. Verifique se o grafico e gerado corretamente

## Troubleshooting

### Container nao inicia

```bash
# Verificar logs de erro
docker-compose logs analytics-chatbot

# Verificar se portas estao em uso
netstat -tulpn | grep 8501
```

### Erro de API Key

```bash
# Verificar se variavel foi carregada
docker-compose exec analytics-chatbot env | grep GEMINI
```

### Health check falhando

```bash
# Testar conectividade interna
docker-compose exec analytics-chatbot curl -f http://localhost:8501/_stcore/health
```

### Memoria insuficiente

Adicione limites de recursos no docker-compose.yml:

```yaml
services:
  analytics-chatbot:
    # ... outras configs
    deploy:
      resources:
        limits:
          memory: 4G
        reservations:
          memory: 2G
```

## Seguranca

- **Nunca** commite o arquivo `.env` no git
- Container roda como usuario nao-root
- Healthcheck habilitados para deteccao de falhas
- GitHub Secrets para CI/CD
- Attestation de proveniencia da imagem

## Estrutura de Volumes

| Volume Local | Container | Modo | Descricao |
|--------------|-----------|------|-----------|
| `./logs` | `/app/logs` | rw | Logs da aplicacao |
| `./data/output` | `/app/data/output` | rw | Graficos exportados |

## Variaveis de Ambiente

| Variavel | Obrigatorio | Default | Descricao |
|----------|-------------|---------|-----------|
| `GEMINI_API_KEY` | Sim | - | API Key do Google Gemini |
| `DATASET_PATH` | Nao | `data/datasets/...parquet` | Caminho do dataset (embutido) |
| `SUPABASE_URL` | Nao | - | URL do Supabase |
| `SUPABASE_API_KEY` | Nao | - | API Key do Supabase |

## CI/CD com GitHub Actions

O workflow `.github/workflows/docker-publish.yml` automatiza:

1. **Push para `main`**: Build e push da imagem com tag `main`
2. **Tags `v*`**: Build e push com versao semantica
3. **Pull Requests**: Build apenas (sem push)

### Configurar GitHub Secret

No repositorio, va em Settings > Secrets and variables > Actions e adicione:

- `GEMINI_API_KEY` - Chave da API Gemini (para testes no CI se necessario)

### Tags Geradas

- `main` - Branch principal
- `v1.0.0` - Versao semantica
- `1.0` - Major.Minor
- `sha-abc1234` - Commit SHA

### Usar Versao Especifica

```bash
# Versao especifica
docker pull ghcr.io/target-solucoes/analytics-chatbot:v1.0.0

# Commit especifico
docker pull ghcr.io/target-solucoes/analytics-chatbot:sha-abc1234
```

## Deploy em Google Cloud Platform (GCP)

Esta secao descreve como realizar o deploy da aplicacao em ambiente GCP, cobrindo as principais opcoes de hospedagem e boas praticas de seguranca.

### Pre-requisitos GCP

- **Conta GCP** com billing habilitado
- **gcloud CLI** instalado e autenticado
- **Projeto GCP** criado
- **APIs habilitadas:**
  - Cloud Run API
  - Artifact Registry API
  - Secret Manager API
  - (Opcional) Google Kubernetes Engine API

```bash
# Autenticar no gcloud
gcloud auth login

# Definir projeto padrao
gcloud config set project SEU_PROJECT_ID

# Habilitar APIs necessarias
gcloud services enable run.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com
```

### Opcao 1: Cloud Run (Recomendado)

Cloud Run e a opcao mais simples para aplicacoes containerizadas serverless. Escala automaticamente de 0 a N instancias.

#### 1.1 Configurar Artifact Registry

```bash
# Criar repositorio de imagens
gcloud artifacts repositories create analytics-chatbot \
  --repository-format=docker \
  --location=us-central1 \
  --description="Analytics Chatbot Docker images"

# Configurar autenticacao do Docker
gcloud auth configure-docker us-central1-docker.pkg.dev
```

#### 1.2 Build e Push da Imagem

```bash
# Tag da imagem para Artifact Registry
IMAGE_URI=us-central1-docker.pkg.dev/SEU_PROJECT_ID/analytics-chatbot/app:latest

# Build da imagem
docker build -t $IMAGE_URI .

# Push para Artifact Registry
docker push $IMAGE_URI
```

#### 1.3 Configurar Secret Manager

```bash
# Criar secret para a API Key
echo -n "sua-gemini-api-key" | gcloud secrets create gemini-api-key \
  --replication-policy="automatic" \
  --data-file=-

# Verificar criacao
gcloud secrets list
```

#### 1.4 Deploy no Cloud Run

```bash
# Deploy com secret montado como variavel de ambiente
gcloud run deploy analytics-chatbot \
  --image=us-central1-docker.pkg.dev/SEU_PROJECT_ID/analytics-chatbot/app:latest \
  --platform=managed \
  --region=us-central1 \
  --port=8501 \
  --memory=2Gi \
  --cpu=2 \
  --min-instances=0 \
  --max-instances=10 \
  --set-secrets=GEMINI_API_KEY=gemini-api-key:latest \
  --allow-unauthenticated
```

| Flag | Proposito |
|------|-----------|
| `--port=8501` | Porta do Streamlit |
| `--memory=2Gi` | Memoria alocada (ajuste conforme necessidade) |
| `--cpu=2` | vCPUs alocadas |
| `--min-instances=0` | Escala para zero quando ocioso (economia) |
| `--max-instances=10` | Limite de escala horizontal |
| `--set-secrets` | Injeta secret como variavel de ambiente |
| `--allow-unauthenticated` | Acesso publico (remova para restringir) |

#### 1.5 Configurar Dominio Customizado (Opcional)

```bash
# Mapear dominio customizado
gcloud run domain-mappings create \
  --service=analytics-chatbot \
  --domain=chatbot.seudominio.com \
  --region=us-central1
```

### Opcao 2: Google Kubernetes Engine (GKE)

Para ambientes que ja utilizam Kubernetes ou precisam de maior controle.

#### 2.1 Criar Cluster GKE

```bash
# Criar cluster Autopilot (gerenciado)
gcloud container clusters create-auto analytics-cluster \
  --region=us-central1 \
  --project=SEU_PROJECT_ID

# Obter credenciais
gcloud container clusters get-credentials analytics-cluster \
  --region=us-central1
```

#### 2.2 Criar Secret no Kubernetes

```bash
# Criar namespace
kubectl create namespace analytics

# Criar secret a partir do Secret Manager (recomendado)
# Ou diretamente:
kubectl create secret generic gemini-credentials \
  --from-literal=GEMINI_API_KEY=sua-gemini-api-key \
  --namespace=analytics
```

#### 2.3 Manifesto Kubernetes

Crie o arquivo `k8s/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: analytics-chatbot
  namespace: analytics
spec:
  replicas: 2
  selector:
    matchLabels:
      app: analytics-chatbot
  template:
    metadata:
      labels:
        app: analytics-chatbot
    spec:
      containers:
      - name: analytics-chatbot
        image: us-central1-docker.pkg.dev/SEU_PROJECT_ID/analytics-chatbot/app:latest
        ports:
        - containerPort: 8501
        env:
        - name: GEMINI_API_KEY
          valueFrom:
            secretKeyRef:
              name: gemini-credentials
              key: GEMINI_API_KEY
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /_stcore/health
            port: 8501
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /_stcore/health
            port: 8501
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: analytics-chatbot-svc
  namespace: analytics
spec:
  type: LoadBalancer
  selector:
    app: analytics-chatbot
  ports:
  - port: 80
    targetPort: 8501
```

#### 2.4 Aplicar Manifesto

```bash
kubectl apply -f k8s/deployment.yaml

# Verificar status
kubectl get pods -n analytics
kubectl get svc -n analytics
```

### Opcao 3: Compute Engine (VM)

Para cenarios que requerem controle total sobre a infraestrutura.

#### 3.1 Criar VM com Container-Optimized OS

```bash
gcloud compute instances create-with-container analytics-chatbot-vm \
  --container-image=us-central1-docker.pkg.dev/SEU_PROJECT_ID/analytics-chatbot/app:latest \
  --container-env=GEMINI_API_KEY=sua-gemini-api-key \
  --machine-type=e2-medium \
  --zone=us-central1-a \
  --tags=http-server \
  --boot-disk-size=20GB
```

#### 3.2 Configurar Firewall

```bash
# Permitir trafego na porta 8501
gcloud compute firewall-rules create allow-streamlit \
  --allow=tcp:8501 \
  --target-tags=http-server \
  --source-ranges=0.0.0.0/0
```

### Gerenciamento de Secrets (Melhores Praticas)

#### Usando Secret Manager com Workload Identity

```bash
# Criar service account
gcloud iam service-accounts create analytics-chatbot-sa \
  --display-name="Analytics Chatbot Service Account"

# Conceder acesso ao secret
gcloud secrets add-iam-policy-binding gemini-api-key \
  --member="serviceAccount:analytics-chatbot-sa@SEU_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

#### Rotacao de Secrets

```bash
# Adicionar nova versao do secret
echo -n "nova-gemini-api-key" | gcloud secrets versions add gemini-api-key \
  --data-file=-

# Cloud Run usa automaticamente a versao "latest"
# Para forcar atualizacao:
gcloud run services update analytics-chatbot --region=us-central1
```

### CI/CD com Cloud Build

Crie o arquivo `cloudbuild.yaml`:

```yaml
steps:
  # Build da imagem
  - name: 'gcr.io/cloud-builders/docker'
    args:
      - 'build'
      - '-t'
      - 'us-central1-docker.pkg.dev/$PROJECT_ID/analytics-chatbot/app:$SHORT_SHA'
      - '-t'
      - 'us-central1-docker.pkg.dev/$PROJECT_ID/analytics-chatbot/app:latest'
      - '.'

  # Push para Artifact Registry
  - name: 'gcr.io/cloud-builders/docker'
    args:
      - 'push'
      - '--all-tags'
      - 'us-central1-docker.pkg.dev/$PROJECT_ID/analytics-chatbot/app'

  # Deploy no Cloud Run
  - name: 'gcr.io/cloud-builders/gcloud'
    args:
      - 'run'
      - 'deploy'
      - 'analytics-chatbot'
      - '--image=us-central1-docker.pkg.dev/$PROJECT_ID/analytics-chatbot/app:$SHORT_SHA'
      - '--region=us-central1'
      - '--platform=managed'

images:
  - 'us-central1-docker.pkg.dev/$PROJECT_ID/analytics-chatbot/app:$SHORT_SHA'
  - 'us-central1-docker.pkg.dev/$PROJECT_ID/analytics-chatbot/app:latest'

options:
  logging: CLOUD_LOGGING_ONLY
```

#### Configurar Trigger

```bash
# Conectar repositorio GitHub
gcloud builds triggers create github \
  --name="analytics-chatbot-deploy" \
  --repo-name="analytics-chatbot" \
  --repo-owner="target-solucoes" \
  --branch-pattern="^main$" \
  --build-config="cloudbuild.yaml"
```

### Monitoramento e Observabilidade

#### Cloud Logging

```bash
# Ver logs do Cloud Run
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=analytics-chatbot" \
  --limit=50 \
  --format="table(timestamp, textPayload)"
```

#### Cloud Monitoring (Alertas)

```bash
# Criar alerta de disponibilidade
gcloud alpha monitoring policies create \
  --display-name="Analytics Chatbot Uptime" \
  --condition-display-name="Uptime check failed" \
  --condition-filter='metric.type="monitoring.googleapis.com/uptime_check/check_passed"' \
  --notification-channels="SEU_CHANNEL_ID"
```

### Custos Estimados (GCP)

| Servico | Configuracao | Custo Estimado/Mes |
|---------|--------------|-------------------|
| Cloud Run | 2GB RAM, 2 vCPU, ~100 req/dia | ~$5-15 |
| GKE Autopilot | 2 pods, 1GB cada | ~$50-80 |
| Compute Engine | e2-medium (2 vCPU, 4GB) | ~$25-35 |
| Secret Manager | 1 secret, 10k acessos | < $1 |
| Artifact Registry | 1GB armazenamento | < $1 |

### Checklist de Deploy GCP

- [ ] Projeto GCP criado e billing habilitado
- [ ] APIs necessarias habilitadas
- [ ] Imagem publicada no Artifact Registry
- [ ] Secret da API Key criado no Secret Manager
- [ ] Service Account configurado com permissoes minimas
- [ ] Deploy realizado (Cloud Run/GKE/Compute Engine)
- [ ] Health check validado
- [ ] Logs funcionando no Cloud Logging
- [ ] (Opcional) Dominio customizado configurado
- [ ] (Opcional) Alertas de monitoramento configurados
- [ ] (Opcional) CI/CD com Cloud Build configurado
