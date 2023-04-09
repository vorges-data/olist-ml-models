WITH tb_pedido AS (

SELECT t1.idPedido,
       t2.idVendedor,
       t1.descSituacao,
       t1.dtPedido,
       t1.dtAprovado,
       t1.dtEntregue,
       t1.dtEstimativaEntrega,
       SUM(vlFrete) AS totalFrete       

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE dtPedido < '{date}'
AND dtPedido >= ADD_MONTHS('{date}', -6)
AND idVendedor IS NOT NULL

GROUP BY t1.idPedido,
         t2.idVendedor,
         t1.descSituacao,
         t1.dtPedido,
         t1.dtAprovado,
         t1.dtEntregue,
         t1.dtEstimativaEntrega
)

SELECT
    '{date}' AS dtReference,
    NOW() AS dtIngestion,
    idVendedor,
    COUNT(DISTINCT CASE WHEN DATE(COALESCE(dtEntregue, '{date}')) > DATE(dtEstimativaEntrega) THEN idPedido END) / COUNT(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS pctPedidoAtraso,
    COUNT(DISTINCT CASE WHEN descSituacao = 'canceled' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoCancelado,
    AVG(totalFrete) AS avgFrete,
    PERCENTILE(totalFrete, 0.5) AS medianFrete,
    MAX(totalFrete) AS maxFrete,
    MIN(totalFrete) AS minFrete,
    AVG(DATEDIFF(COALESCE(dtEntregue, '{date}'), dtAprovado)) AS qtdDiasAprovadoEntrega,
    AVG(DATEDIFF(COALESCE(dtEntregue, '{date}'), dtPedido)) AS qtdDiasPedidoEntrega,
    AVG(DATEDIFF(dtEstimativaEntrega, COALESCE(dtEntregue, '{date}'))) AS qtdeDiasEntregaPromessa
       
FROM tb_pedido

GROUP BY 1,2,3