use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(User::Table)
                    .if_not_exists()
                    .col(pk_auto(User::Id))
                    .col(string(User::Did).not_null())
                    .col(string(User::Username).not_null())
                    .col(string(User::DisplayName).not_null())
                    .col(string(User::DeviceIds).not_null())
                    .col(text(User::PublicKeyJwk).null())
                    .col(timestamp_with_time_zone(User::TimeCreated).not_null())
                    .col(timestamp_with_time_zone(User::LastLogin).null())
                    .to_owned(),
            )
            .await?;

        // Create unique index on DID for fast lookups
        manager
            .create_index(
                Index::create()
                    .name("idx_user_did")
                    .table(User::Table)
                    .col(User::Did)
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_user_did")
                    .table(User::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(Table::drop().table(User::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum User {
    Table,
    Id,
    Did,
    Username,
    DisplayName,
    DeviceIds,
    PublicKeyJwk,
    TimeCreated,
    LastLogin,
}
