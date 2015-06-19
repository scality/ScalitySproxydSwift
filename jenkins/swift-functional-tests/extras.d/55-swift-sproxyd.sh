# 55-swift-sproxyd.sh - Devstack extras script to configure s-object with swiftsproxyd driver

function install_sproxyd_driver {
    # Get the correct sproxyd-client version from the requirements
    # Currently retrieved from test-requirements.txt, which is a bit of a hack
    local scal_sproxyd_client=$(grep 'scality-sproxyd-client' ${WORKSPACE}/test-requirements.txt)
    if [[ -n "$scal_sproxyd_client" ]]; then
        sudo pip install "$scal_sproxyd_client"
    fi
    # For some reason, doing this failed: keystone would not work,
    # complaining about "ArgsAlreadyParsedError: arguments already parsed:".
    #sudo python setup.py install
    sudo pip install .
}

# Shameless ripoff of devstack/lib/swift
function enable_sproxyd_driver {
    for node_number in ${SWIFT_REPLICAS_SEQ}; do
        local swift_node_config=${SWIFT_CONF_DIR}/object-server/${node_number}.conf
        iniset ${swift_node_config} app:object-server use egg:swift_scality_backend#sproxyd_object
        # Host and port need to be configurable
        iniset ${swift_node_config} app:object-server sproxyd_host 127.0.0.1:81
        # /proxy_path need to be configurable
        iniset ${swift_node_config} app:object-server sproxyd_path /proxy/chord_path
        # splice need to be configurable
        iniset ${swift_node_config} app:object-server splice yes
    done
}

function amend_swift_conf {
    local swift_conf=${SWIFT_CONF_DIR}/swift.conf
    iniset ${swift_conf} storage-policy:1 name Policy-1
}

function create_storage_policies_conf {
    cat <<EOF >${SWIFT_CONF_DIR}/scality-storage-policies.ini
[ring:paris-arc]
location = paris
sproxyd_endpoints = http://127.0.0.1:81/proxy/arc

[storage-policy:1]
read = paris-arc
write = paris-arc

EOF
}

function symlink_ring_files {
    ln -s ${SWIFT_CONF_DIR}/object.ring.gz ${SWIFT_CONF_DIR}/object-1.ring.gz
}

function enable_storage_policies {
    amend_swift_conf
    create_storage_policies_conf
    symlink_ring_files
}

if is_service_enabled s-object; then
    if [[ "$1" == "stack" && "$2" == "install" ]]; then
        echo_summary "Post-config hook : install swift-sproxyd."
        install_sproxyd_driver
    fi
    if [[ "$1" == "stack" && "$2" == "post-config" ]]; then
        echo_summary "Post-config hook : enable swift-sproxyd."
        enable_sproxyd_driver
        enable_storage_policies
    fi
fi
