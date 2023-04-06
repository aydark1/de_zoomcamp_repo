terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 0.13"
}

provider "yandex" {
  zone = var.zone
}

resource "yandex_iam_service_account_static_access_key" "sa-static-key" {
  service_account_id = var.service_account_id
  description        = "static access key for object storage"
}

// Use keys to create bucket
resource "yandex_storage_bucket" "test" {
  access_key = yandex_iam_service_account_static_access_key.sa-static-key.access_key
  secret_key = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
  bucket     = var.bucket
}


resource "yandex_mdb_postgresql_database" "foo" {
  cluster_id = yandex_mdb_postgresql_cluster.foo.id
  name       = "kaggle_youtube_data"
  owner      = yandex_mdb_postgresql_user.foo.name
  lc_collate = "en_US.UTF-8"
  lc_type    = "en_US.UTF-8"
}

resource "yandex_mdb_postgresql_user" "foo" {
  cluster_id = yandex_mdb_postgresql_cluster.foo.id
  name       = "*****"
  password   = "*****"
}


resource "yandex_mdb_postgresql_cluster" "foo" {
  name        = "de_zoomcamp_pg_cluster"
  environment = "PRESTABLE"
  network_id  = var.network_id

  config {
    version = 15
    resources {
      resource_preset_id = "c3-c2-m4"
      disk_type_id       = "network-ssd"
      disk_size          = 10
    }

  }

  maintenance_window {
    type = "ANYTIME"
  }

  host {
    zone      = var.zone
    subnet_id = var.subnet_id
    assign_public_ip = true
  }
}



