ClusterCreateProperties(
    cluster_version="4.0",
    os_type=OSType.linux,
    tier=Tier.standard,
    cluster_definition=ClusterDefinition(
        kind="spark",
        configurations={
            "gateway": {
                "restAuthCredential.enabled_credential": "True",
                "restAuthCredential.username": "{{ params.CLUSTER_LOGIN_USER_NAME }}",
                "restAuthCredential.password": "{{ params.CLUSTER_PASSWORD }}"
            }
        }
    ),
    compute_profile=ComputeProfile(
        roles=[
            Role(
                name="headnode",
                target_instance_count=2,
                hardware_profile=HardwareProfile(vm_size="Large"),
                os_profile=OsProfile(
                    linux_operating_system_profile=LinuxOperatingSystemProfile(
                        username="{{ params.SSH_USER_NAME }}",
                        password="{{ params.SSH_PASSWORD }}"
                    )
                )
            ),
            Role(
                name="workernode",
                target_instance_count=1,
                hardware_profile=HardwareProfile(vm_size="Large"),
                os_profile=OsProfile(
                    linux_operating_system_profile=LinuxOperatingSystemProfile(
                        username="{{ params.SSH_USER_NAME }}",
                        password="{{ params.SSH_PASSWORD }}"
                    )
                )
            )
        ]
    ),
    storage_profile=StorageProfile(
        storageaccounts=[StorageAccount(
            name="{{ params.STORAGE_ACCOUNT_NAME }}" + '.blob.core.windows.net',
            key="{{ params.STORAGE_ACCOUNT_KEY }}",
            container="{{ params.CONTAINER_NAME }}",
            is_default=True
        )]
    )
)