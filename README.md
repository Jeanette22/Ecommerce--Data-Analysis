# Ecommerce--Data-Analysis
## Utilizo Hadoop- Spark- HDFS -Pyspark para analizar dataset sobre transacciones de un emprendimiento 

##### Conjunto de datos de transacciones que contiene todas las transacciones que ocurrieron entre el 01/12/2010 y el 09/12/2011 para una venta minorista en línea no basada en una tienda física y registrada en el Reino Unido. La empresa principalmente vende regalos únicos para todas las ocasiones. Muchos clientes de la empresa son mayoristas

![1](https://user-images.githubusercontent.com/80054717/225991537-5fd5231f-cec7-4ce1-bbbb-7e0efcddd98a.png)


datos_a.createOrReplaceTempView("tabla_datos")  *crear vista
![2](https://user-images.githubusercontent.com/80054717/225991599-4db267b9-9496-46cc-8996-eee13f791201.png)


#### Podemos utilizar la función "col" para referirnos a una columna en un dataframe de una manera más eficiente y legible. 
Estamos utilizando la función "col" para seleccionar las columnas del dataframe "datos_a" y convertir las columnas "Quantity" y "CustomerID" a tipo integer. Al utilizar la función "col", podemos referirnos a las columnas del dataframe de una manera clara y concisa.
También se pueden utilizar otras funciones de Pyspark como "lit", "concat", "substring", "sum", entre otras, junto con la función "col" para realizar operaciones más complejas en las columnas del dataframe.
![3](https://user-images.githubusercontent.com/80054717/225991670-50d892ab-98b9-4702-bb4e-f9d9e2da02fa.png)



*para hacer consultas con datos numéricos, se suele recomendar cambiarles el tipo de dato en caso de que esten en “string”*

Total de facturas únicas: 25900

![4](https://user-images.githubusercontent.com/80054717/225991754-df912fa9-2d45-4274-b47a-4de418f56496.png)


La cantidad total de productos vendidos: 5.176.450.0
![5](https://user-images.githubusercontent.com/80054717/225991805-3c58254a-2b97-43be-881a-ac13c2aa19e4.png)


Cantidad de productos vendidos por país:
![6](https://user-images.githubusercontent.com/80054717/225991830-dd9e39b3-8199-43c8-a6e9-a3ff1500e3bd.png)


Obtener el promedio de precios unitarios de los productos: 4.611113626082972
![7](https://user-images.githubusercontent.com/80054717/225991857-905d4fee-74ab-4be1-becf-44c56af22c4a.png)


la cantidad de facturas por día: 541.909
![8](https://user-images.githubusercontent.com/80054717/225991886-da31d94f-f7d7-420a-ad40-d16e124ee8c9.png)


Número total de transacciones en el dataset:
![9](https://user-images.githubusercontent.com/80054717/225991920-1f668896-9cb7-47f5-9ed0-7f64655efe3a.png)


Número de clientes únicos:
![10](https://user-images.githubusercontent.com/80054717/225991944-8b5d1f95-316c-4916-b5e8-877eb4d0f59b.png)


Productos más vendidos:
![11](https://user-images.githubusercontent.com/80054717/225991962-2b76aa2e-2ea8-4754-99a4-467740614079.png)


Transacciones realizadas en un país en particular: Francia
![12](https://user-images.githubusercontent.com/80054717/225991984-eb88945f-4d92-4226-8865-22f6868511bc.png)


Análisis de ventas por mes: 9747747.93
![13](https://user-images.githubusercontent.com/80054717/225992032-2e021248-1f25-4265-9ee7-faf3360236d1.png)


Obtener la lista de los 10 clientes que más gastaron: ID
![14](https://user-images.githubusercontent.com/80054717/225992058-d0413e46-056f-40e7-9234-eb7def6b5cd4.png)


