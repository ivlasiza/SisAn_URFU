import luigi
import requests
import tarfile
import os
import gzip
import shutil
import subprocess
import logging
from tqdm import tqdm

# Настройка ведения журнала
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DownloadFile(luigi.Task):
    """
    Задача для загрузки файла по заданному URL.
    """

    url = luigi.Parameter()
    output_dir = luigi.Parameter()

    def output(self):
        # Определение местоположения для локального целевого файла
        return luigi.LocalTarget(os.path.join(self.output_dir, "archive.tar"))

    def run(self):
        # Создание выходной директории, если она не существует
        os.makedirs(self.output_dir, exist_ok=True)

        # Запрос на загрузку файла
        response = requests.get(self.url, stream=True)
        total_size = int(response.headers.get("content-length", 0))

        # Запись загруженного файла
        with open(self.output().path, "wb") as f:
            for data in tqdm(
                response.iter_content(1024),
                total=total_size // 1024,
                desc="Downloading",
            ):
                f.write(data)

        logging.info(f"File downloaded to {self.output().path}")


class VerifyAndExtractTar(luigi.Task):
    """
    Задача для проверки корректности и извлечения TAR-файла.
    """

    url = luigi.Parameter()
    output_dir = luigi.Parameter()

    def requires(self):
        # Указывает, что для этой задачи требуется предыдущая задача DownloadFile
        return DownloadFile(self.url, self.output_dir)

    def output(self):
        # Определение местоположения для директории извлеченных файлов
        return luigi.LocalTarget(os.path.join(self.output_dir, "extracted"))

    def run(self):
        tar_file_path = self.input().path
        extract_dir = self.output().path

        # Создание директории для извлеченных файлов
        os.makedirs(extract_dir, exist_ok=True)

        try:
            # Проверка целостности TAR-файла
            with tarfile.open(tar_file_path, "r") as tar_ref:
                tar_ref.getmembers()
            logging.info(f"TAR file {tar_file_path} passed integrity check.")
        except Exception as e:
            raise Exception(f"TAR file integrity check failed: {e}")

        # Извлечение содержимого TAR-файла
        with tarfile.open(tar_file_path, "r") as tar_ref:
            tar_ref.extractall(path=extract_dir)
        logging.info(f"Extracted files to {extract_dir}")


class OrganizeAndProcessFiles(luigi.Task):
    """
    Задача для организации и обработки извлеченных файлов.
    """

    url = luigi.Parameter()
    output_dir = luigi.Parameter()
    delete_archives = luigi.BoolParameter(default=True)

    def requires(self):
        # Указывает, что для этой задачи требуется предыдущая задача VerifyAndExtractTar
        return VerifyAndExtractTar(self.url, self.output_dir)

    def output(self):
        # Определение местоположения для директории обработанных файлов
        return luigi.LocalTarget(os.path.join(self.output_dir, "processed"))

    def run(self):
        extracted_dir = self.input().path
        processed_dir = self.output().path

        # Создание директории для обработанных файлов
        os.makedirs(processed_dir, exist_ok=True)

        # Обработка каждого файла в извлеченной директории
        for file_name in os.listdir(extracted_dir):
            file_path = os.path.join(extracted_dir, file_name)
            base_name, ext = os.path.splitext(file_name)
            file_folder = os.path.join(processed_dir, base_name)

            # Создание директории для каждого процесса файла
            os.makedirs(file_folder, exist_ok=True)

            if ext == ".gz":
                # Декомпрессия GZ-файла
                decompressed_file = os.path.join(file_folder, base_name)
                with gzip.open(file_path, "rb") as f_in:
                    with open(decompressed_file, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                logging.info(f"Decompressed {file_name} to {decompressed_file}")
            else:
                # Копирование других файлов
                shutil.copy(file_path, file_folder)
                logging.info(f"Copied {file_name} to {file_folder}")

        # Удаление оригинальной извлеченной директории, если это стоит в параметрах
        if self.delete_archives:
            shutil.rmtree(extracted_dir)
            logging.info(f"Deleted original extracted folder {extracted_dir}")

        logging.info(f"Organized files into {processed_dir}")


class RunExternalScript(luigi.Task):
    """
    Задача для выполнения внешнего скрипта над обработанными файлами.
    """

    url = luigi.Parameter()
    output_dir = luigi.Parameter()
    script_path = luigi.Parameter(default="toTabs.py")

    def requires(self):
        # Указывает, что для этой задачи требуется предыдущая задача OrganizeAndProcessFiles
        return OrganizeAndProcessFiles(self.url, self.output_dir)

    def output(self):
        # Определение местоположения для директории завершенных процессов
        return luigi.LocalTarget(os.path.join(self.output_dir, "completed"))

    def run(self):
        processed_dir = self.input().path

        # Создание директории для завершенных результатов
        os.makedirs(self.output().path, exist_ok=True)

        # Выполнение скрипта для каждого папки с обработанными файлами
        for folder in os.listdir(processed_dir):
            folder_path = os.path.join(processed_dir, folder)
            file_path = os.path.join(
                folder_path, folder
            )  # Предполагается, что в каждой папке один файл

            if os.path.isfile(file_path):
                try:
                    # Выполнение внешнего скрипта
                    subprocess.run(
                        ["python3.9", self.script_path, file_path], check=True
                    )
                    logging.info(f"Processed {file_path} with {self.script_path}")
                except subprocess.CalledProcessError as e:
                    logging.error(f"Error processing {file_path}: {e}")
            else:
                logging.warning(f"Skipped {folder_path}: Not a file.")

        logging.info(f"All files processed. Results stored in {processed_dir}")


if __name__ == "__main__":
    luigi.run()
