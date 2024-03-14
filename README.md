# Repository: cadastro-materiais-gov

## Description

This GitHub repository hosts a PySpark application designed for capturing and processing data from [Compras Governamentais](https://catalogo.compras.gov.br/) and [ComprasNet](https://compras.dados.gov.br/), with a specific focus on materials classified under group 65: "Equipamentos E Artigos Para Uso Médico, Dentário E Veterinário"
. The application utilizes the medallion approach to curate the data, ensuring high-quality and reliable information.

## Features

- **Data Capture and Processing:** The primary function of this application is to retrieve and process data related to government procurements, specifically materials falling under group 65.

- **Medallion Approach:** The application implements the medallion approach to curate the data, enhancing its reliability and accuracy.

- **Google Cloud Platform Integration:** Data is stored in a Google Cloud Storage bucket, providing a scalable and reliable solution for data storage.

- **Docker Support:** The repository includes a Dockerfile for containerization. Ensure the `gcp_key_compras_bucket.json` file is stored in the `utils` folder before building and running the Docker container.

- **Data Quality Assurance:** The `check` folder incorporates Great Expectations to ensure data quality, allowing users to validate and guarantee the integrity of the processed data.

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/ricardonascimentosoares/cadastro-materiais-gov.git
2. Navigate to the project directory:

    ```bash
    cd cadastro-materiais-gov
3. Place the gcp_key_compras_bucket.json file in the utils folder.

4. Build and run the Docker container:

    ```bash
    docker build -t cadastro-materiais-gov .
    docker run -it cadastro-materiais-gov

## Data Output

This project generates 3 files in the output folder, extracted from the Gold Layer. They are available to download:

1. [material_agg.xlsx](https://storage.googleapis.com/compras-bucket/output/material_agg.xlsx):  Quantities of materials items grouped by PDM and Classe.

2. [material_char_detail.xlsx](https://storage.googleapis.com/compras-bucket/output/material_char_detail.xlsx): Data at grain of Characteristics from material items.
3. [material_list.xlsx](https://storage.googleapis.com/compras-bucket/output/material_list.xlsx): Analytical data showing the info of materials items
