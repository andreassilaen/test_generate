from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
import logging

# Inisialisasi logger
logger = logging.getLogger("airflow.task")# Inisialisasi logger

# Default arguments untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

current_date = datetime.now() - timedelta(days=1)  # Mengambil tanggal hari Kemarin
# -> Mendapatkan hari dalam format satu atau dua digit
current_day = current_date.strftime('%d')  # Ini akan mengeluarkan 9 untuk 9 Januari, atau 13 untuk 13 Januari
# -> Mendapatkan bulan dalam format tiga huruf (misalnya 'jan' untuk Januari)
current_month_eng = current_date.strftime('%b').lower()
current_month_id = current_date.strftime('%b').capitalize()
# -> Mendapatkan tahun dalam format empat digit
current_year = current_date.strftime('%Y')

# Mendapatkan bulan dalam bahasa Indonesia
bulan_indonesia = {
    "jan": "Januari",
    "feb": "Februari",
    "mar": "Maret",
    "apr": "April",
    "may": "Mei",
    "jun": "Juni",
    "jul": "Juli",
    "aug": "Agustus",
    "sep": "September",
    "oct": "Oktober",
    "nov": "November",
    "dec": "Desember"
}


# Mengambil nama bulan dalam bahasa Indonesia dari dictionary
current_month_id = bulan_indonesia[current_month_eng]

s3_class_path = f"s3://s3_testing_bucket/folder1/folder2/test_sep_class_2024.csv"
s3_path = f"s3://s3_testing_bucket/folder1/folder2/test_sep_2024.csv"


# Definisi DAG
with DAG(
    dag_id='DEV_Data_Get_Lionair_App_To_Redshift_FlightSummary_PerMonth',
    default_args=default_args,
    description='Menjalankan query SQL di Redshift (insert s3 to redshift lionair schema)',
    # schedule_interval='20 6 * * *',  # 6:20 UTC equals 1:20 PM WIB
    schedule_interval=None,
    tags=["lionair", "flight_summary", "trigger manually"],
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:


    # Task untuk menjalankan query SQL
    execute_redshift_sql = RedshiftDataOperator(
        task_id='execute_redshift_sql',
        cluster_identifier='cluster',  # Nama cluster Redshift Anda
        database='tony',  # Database Redshift
        db_user='admin_tony',  # User Redshift ************ Ganti dengan user Redshift Anda
        sql="""
        drop table if exists public.flight_class_summary;

        CREATE TABLE public.flight_class_summary (
            "Date" VARCHAR(MAX),
            Provinsi VARCHAR(MAX),
            FlightNo VARCHAR(MAX),
            Route VARCHAR(MAX),
            Class VARCHAR(MAX),
            Pax_Vie VARCHAR(MAX),
            NTA_Publish_VIE VARCHAR(MAX),
            SurCharge_VIE VARCHAR(MAX),
            Pax_Mnfst VARCHAR(MAX),
            NTA_Publish_Mnfst VARCHAR(MAX),
            SurCharge_Mnfst VARCHAR(MAX)
        );

        COPY public.flight_class_summary
        FROM
            '{{ params.s3_class_path }}'
        iam_role 'arn:aws:iam::888888888:role/tony_dev'
        CSV DELIMITER ',' ACCEPTINVCHARS emptyasnull blanksasnull IGNOREHEADER 1;

        drop table if exists public.flight_class_summary_temp2;
        CREATE TABLE public.flight_class_summary_temp2 as (
            select
                "date"::date as "date",
                provinsi,
                flightno,
                route,
                class,
                pax_vie::int AS pax_vie,
                nta_publish_vie::numeric(20,2) as nta_publish_vie,
                surcharge_vie::numeric(20,2) as surcharge_vie,
                pax_mnfst::int as pax_mnfst,
                nta_publish_mnfst::numeric(20,2) as nta_publish_mnfst,
                surcharge_mnfst::numeric(20,2) as surcharge_mnfst
            from
                public.flight_class_summary
        );

        delete from summary.flight_class_summary_v1
        where to_char("date", 'yyyy-mm')='2024-09';
        insert into summary.flight_class_summary_v1 (select * from public.flight_class_summary_temp2);

        drop table if exists public.flight_class_summary;
        drop table if exists public.flight_class_summary_temp2;

        DROP TABLE if exists public.flight_summary;

        CREATE TABLE public.flight_summary (
            "date" VARCHAR(MAX),
            provinsi VARCHAR(MAX),
            airlines VARCHAR(MAX),
            flightno VARCHAR(MAX),
            route VARCHAR(MAX),
            flight_hour VARCHAR(MAX),
            tba VARCHAR(MAX),
            pax_vie VARCHAR(MAX),
            nta_publish_vie VARCHAR(MAX),
            surcharge_vie VARCHAR(MAX),
            perhour_vie VARCHAR(MAX),
            pax_mnfst VARCHAR(MAX),
            nta_publish_mnfst VARCHAR(MAX),
            surcharge_mnfst VARCHAR(MAX),
            perhour_mnfst VARCHAR(MAX),
            diffent VARCHAR(MAX),
            std VARCHAR(MAX)
        );

        COPY public.flight_summary
        FROM
            '{{ params.s3_path }}'
        iam_role 'arn:aws:iam::888888888:role/tony_dev'
        CSV DELIMITER ',' ACCEPTINVCHARS emptyasnull blanksasnull IGNOREHEADER 1;

        DROP TABLE if exists public.flight_summary_temp2;
        CREATE TABLE public.flight_summary_temp2 AS (
            SELECT
                "date"::date as "date",
                provinsi,
                airlines,
                flightno,
                route,
                flight_hour,
                tba::int as tba,
                pax_vie::int as pax_vie,
                CAST(nta_publish_vie AS DECIMAL(20, 2)) AS nta_publish_vie,
                CAST(surcharge_vie AS DECIMAL(20, 2)) AS surcharge_vie,
                CASE
                    WHEN perhour_vie = 'Not Available' THEN NULL
                    ELSE CAST(perhour_vie AS DECIMAL(20, 2))
                END AS perhour_vie,
                pax_mnfst::float::int AS pax_mnfst,
                CAST(nta_publish_mnfst AS DECIMAL(20, 2)) AS nta_publish_mnfst,
                CAST(surcharge_mnfst AS DECIMAL(20, 2)) AS surcharge_mnfst,
                CASE
                    WHEN perhour_mnfst = 'Not Available' THEN NULL
                    ELSE CAST(perhour_mnfst AS DECIMAL(20, 2))
                END AS perhour_mnfst,
                diffent,
                std,
        
                -- Menghitung flight_summary_timestamp
                CASE 
                    WHEN std ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' 
                        THEN ("date" || ' ' || std)::timestamp  -- Format jam valid (00:00 - 23:59)
                    WHEN std ~ '^[2-9][4-9]:[0-9]{2}$' OR std ~ '^[0-9]{2}:[6-9][0-9]$' OR std ~ '^[3-9][0-9]:'
                        THEN ("date"::date  
                            + (SPLIT_PART(std, ':', 1)::int * 60 + SPLIT_PART(std, ':', 2)::int) * INTERVAL '1 minute')
                    WHEN std = '0' THEN ("date" || ' 00:00')::timestamp
                    WHEN std ~ '[A-Za-z-]' THEN NULL  -- Jika mengandung huruf atau simbol
                    ELSE NULL 
                END AS flight_summary_timestamp,
                
                -- Menandai apakah data normal atau anomali
                CASE 
                    WHEN std ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' 
                        THEN 'normal'  -- Data dalam 24 jam
                    WHEN std ~ '^[2-9][4-9]:[0-9]{2}$' OR std ~ '^[0-9]{2}:[6-9][0-9]$' OR std ~ '^[3-9][0-9]:'
                        THEN 'anomaly' -- Data lebih dari 24 jam
                    WHEN std ~ '[A-Za-z-]' THEN 'anomaly' -- Jika mengandung karakter non-numeric
                    WHEN std = '0' THEN 'anomaly'
                    ELSE NULL 
                END AS std_flag
            FROM public.flight_summary
        );

        DELETE FROM
            summary.flight_summary_v1
        WHERE
            to_char(date :: DATE, 'yyyy-mm') = '2024-09';

        insert into
            summary.flight_summary_v1 (
                Select
                    *
                from
                    public.flight_summary_temp2
            );

        drop table if exists public.flight_class_summary;
        drop table if exists public.flight_summary_temp2;
        """,
        params={
        's3_class_path': s3_class_path,  # Pastikan s3_class_path didefinisikan di sini
        's3_path': s3_path  # Jika perlu, Anda juga bisa menambahkan s3_path
        },
        # Callback saat task berhasil
        on_success_callback=lambda context: logger.info(f"Query berhasil dijalankan ke Redshift.\n"
                                                        f"S3 Class Path: {s3_class_path}\n"
                                                        f"S3 Path: {s3_path}"),
        # Callback saat task gagal
        on_failure_callback=lambda context: logger.error(f"Query gagal dijalankan ke Redshift.\n"
                                                         f"S3 Class Path: {s3_class_path}\n"
                                                         f"S3 Path: {s3_path}"),
    )

    # log_s3_task >> execute_redshift_sql
    execute_redshift_sql
