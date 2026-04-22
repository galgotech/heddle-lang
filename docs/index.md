Componentes:

* Control Panel (golang)
  * Compilação do heddle lang para IR para enviar para workers
  * Responsável por injetar as funções em cada worker
  * Responsável por gerenciar qual worker está com um resource aberto
  * Otimizar o DAG e decidir quais workers irão processar o dag.
  * Repassar as variáveis de ambiente para o worker conseguir conectar e fazer o que precisa.

* Worker (python, golang, rust, nodejs)
  * Realiza o processamento de cada step
  * Sincroniza os dados com data manager.
  * Resource a conexão com o banco de dados precisa ficar aberta até o timeout do resource sincronizado com o controle plane que gerencia esses resources.
  * Diferentes worker precisam de dado irão fazer uma busca p2p, e depois do DAG executado todos os dados imutabales são limpos. Salvo somente o que foi definido no workflow.

* Data Memory/manager (golang) https://v6d.io/docs.html
  * Lidar com offload
  * Mesmos recursos do Vineyard.

Comunicação:
  * Entre control panel <-> worker (GRPC/RPC)?
  * Worker <-> Data Manager ( gRPC/RPC metdata, Apache Arrow flight)?