# Previsão de Ônibus no Rio de Janeiro

Este projeto visa prever a localização futura de ônibus no Rio de Janeiro com base em dados históricos de localização e horário e faz parte da disciplina de Data Mining (CPS833) do PESC UFRJ. 

## Tecnologias Utilizadas

- PostgreSQL
- SQLAlchemy (Python)
- Python 3.x


## Modelo

O modelo se baseia na identificação de padrões históricos para prever a localização futura dos ônibus. 
Utilizamos dados históricos de localização e tempo dos ônibus no Rio de Janeiro para encontrar registros passados que tenham características similares às atuais, como linha e localização aproximada. 
A partir desses registros, selecionamos os pontos que têm um comportamento de movimento semelhante e calculamos a mediana das coordenadas de localização futuras, 
para fornecer uma previsão de onde o ônibus estará em um momento futuro. Este método permite uma previsão baseada em padrões reais observados, aumentando a precisão das previsões.
