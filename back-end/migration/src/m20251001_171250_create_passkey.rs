use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(PassKey::Table)
                    .if_not_exists()
                    .col(pk_auto(PassKey::Id))
                    .col(integer(PassKey::UserId).null())
                    .col(string(PassKey::DeviceId).not_null())
                    .col(blob(PassKey::CredentialId).not_null())
                    .col(blob(PassKey::PublicKey).not_null())
                    .col(integer(PassKey::SignCount).default(0).not_null())
                    .col(integer(PassKey::AuthenticationCount).default(0).not_null())
                    .col(timestamp_with_time_zone(PassKey::LastAuthenticated).null())
                    .col(string(PassKey::Name).null())
                    .col(string(PassKey::Attestation).null())
                    .col(text(PassKey::JsonData).not_null())
                    .col(timestamp_with_time_zone(PassKey::TimeCreated).not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_passkey_user")
                            .from(PassKey::Table, PassKey::UserId)
                            .to(User::Table, User::Id)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        // Create unique index on credential_id
        manager
            .create_index(
                Index::create()
                    .name("idx_passkey_credential_id")
                    .table(PassKey::Table)
                    .col(PassKey::CredentialId)
                    .unique()
                    .to_owned(),
            )
            .await?;

        // Create index on device_id for fast lookups during registration
        manager
            .create_index(
                Index::create()
                    .name("idx_passkey_device_id")
                    .table(PassKey::Table)
                    .col(PassKey::DeviceId)
                    .to_owned(),
            )
            .await?;

        // Create index on user_id for fast lookups
        manager
            .create_index(
                Index::create()
                    .name("idx_passkey_user_id")
                    .table(PassKey::Table)
                    .col(PassKey::UserId)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_passkey_user_id")
                    .table(PassKey::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_index(
                Index::drop()
                    .name("idx_passkey_device_id")
                    .table(PassKey::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_index(
                Index::drop()
                    .name("idx_passkey_credential_id")
                    .table(PassKey::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(Table::drop().table(PassKey::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum PassKey {
    Table,
    Id,
    UserId,
    DeviceId,
    CredentialId,
    PublicKey,
    SignCount,
    AuthenticationCount,
    LastAuthenticated,
    Name,
    Attestation,
    JsonData,
    TimeCreated,
}

#[derive(DeriveIden)]
enum User {
    Table,
    Id,
}
