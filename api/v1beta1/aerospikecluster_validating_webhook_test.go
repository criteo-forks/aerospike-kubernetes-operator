package v1beta1

import (
	"testing"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var aslog = logf.Log.WithName("Test validateNsConfUpdate")

// validateNsConfUpdate should return nil because we are adding a new namespace with unused devices
func TestAddPesistentNamespaceWithNotUsedDevices(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0": {"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newNsConf := map[string][]string{
		"namespace-0":   {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace": {"/opt/data02/xvd01", "/opt/data02/xvd02"},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err != nil {
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	}
}

// validateNsConfUpdate should return an error because we are using an already used device
func TestAddPesistentNamespaceWithAlreadyUsedDevices(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0": {"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newNsConf := map[string][]string{
		"namespace-0":   {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace": {"/opt/data01/xvd01", "/opt/data02/xvd02"}, // <- Using same device "/opt/data01/xvd01" as namespace-0
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	}
}

// validateNsConfUpdate should return nil because we are adding multiple namespaces with unused devices
func TestAddMultiplePesistentNamespacesWithNotUsedDevices(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0": {"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newNsConf := map[string][]string{
		"namespace-0":     {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": {"/opt/data02/xvd01", "/opt/data02/xvd02"},
		"new-namespace-1": {"/opt/data02/xvd03", "/opt/data02/xvd04"},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err != nil {
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	}
}

// validateNsConfUpdate should return an error because we are adding multiple namespaces with already used devices
func TestAddMultiplePesistentNamespacesWithAlreadyUsedDevices(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0": {"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newNsConf := map[string][]string{
		"namespace-0":     {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": {"/opt/data02/xvd01", "/opt/data02/xvd02"},
		"new-namespace-1": {"/opt/data02/xvd01", "/opt/data02/xvd04"}, // <- Using same device "/opt/data02/xvd01" as new-namespace-0
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	}
}

// validateNsConfUpdate should return nil because we are adding unused devices to an existing namespace
func TestAddDevicesToExistantNamespace(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0":     {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": {"/opt/data02/xvd01", "/opt/data02/xvd02"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": {
			"/opt/data02/xvd01", "/opt/data02/xvd02",
			"/opt/data02/xvd03", "/opt/data02/xvd04",
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err != nil {
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	}
}

// validateNsConfUpdate should return an error because we are adding used devices by another namespace to an existing namespace
func TestAddUsedDeviceByAnotherNamespaceToExistantNamespace(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0":     {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": {"/opt/data02/xvd01", "/opt/data02/xvd02"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": {
			"/opt/data02/xvd01", "/opt/data02/xvd02",
			"/opt/data01/xvd01", "/opt/data02/xvd04", // <- Using same device "/opt/data01/xvd01" as namespace-0
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	}
}

// validateNsConfUpdate should return an error because we are adding used devices by same namespace to an existing namespace
func TestAddUsedDeviceBySameNamespaceToExistantNamespace(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0":     {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": {"/opt/data02/xvd01", "/opt/data02/xvd02"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": {
			"/opt/data02/xvd01", "/opt/data02/xvd02",
			"/opt/data02/xvd01", "/opt/data02/xvd04", // <- Using same device "/opt/data02/xvd01" as new-namespace-0
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	}
}

// validateNsConfUpdate should return nil because we are adding unused devices to an existing namespace
func TestAddDevicesToMultipleExistantNamespaces(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0":     {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": {"/opt/data02/xvd01", "/opt/data02/xvd02"},
		"new-namespace-1": {"/opt/data03/xvd01", "/opt/data03/xvd02"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": {
			"/opt/data02/xvd01", "/opt/data02/xvd02",
			"/opt/data02/xvd03", "/opt/data02/xvd04",
		},
		"new-namespace-1": {
			"/opt/data03/xvd01", "/opt/data03/xvd02",
			"/opt/data03/xvd03", "/opt/data03/xvd04",
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err != nil {
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	}
}

// validateNsConfUpdate should return an error because we are adding used devices to an existing namespace
func TestAddUsedDeviceToMultipleExistantNamespace(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0":     {},
		"new-namespace-0": {"/opt/data02/xvd01", "/opt/data02/xvd02"},
		"new-namespace-1": {"/opt/data03/xvd01", "/opt/data03/xvd02"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": {
			"/opt/data02/xvd01", "/opt/data02/xvd02",
			"/opt/data02/xvd03", "/opt/data01/xvd01",
		},
		"new-namespace-1": {
			"/opt/data03/xvd01", "/opt/data03/xvd02",
			"/opt/data02/xvd03", "/opt/data03/xvd04", // <- Using same device "/opt/data02/xvd03" as new-namespace-0
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	}
}

// validateStorageConfiguration should return nil because we are adding unused devices and keeping the same type.
func TestValidateStorageConfiguration(t *testing.T) {
	oldStorage := map[string]interface{}{
		"type":    "device",
		"devices": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newStorage := map[string]interface{}{
		"type": "device",
		"devices": []string{
			"/opt/data01/xvd01", "/opt/data01/xvd02",
			"/opt/data01/xvd03", "/opt/data01/xvd04",
		},
	}

	err := validateStorageConfiguration(oldStorage, newStorage, "test-namespace")
	if err != nil {
		t.Fatalf("Got error while validating storage configuration: %v", err)
	}
}

// validateStorageConfiguration should return an error because we are changing the type of the storage.
func TestValidateStorageConfigurationByChangingStorageType(t *testing.T) {
	oldStorage := map[string]interface{}{
		"type":    "device",
		"devices": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newStorage := map[string]interface{}{
		"type":    "memory",
		"devices": []string{},
	}

	err := validateStorageConfiguration(oldStorage, newStorage, "test-namespace")
	if err == nil {
		t.Fatalf("Got error while validating storage configuration: %v", err)
	}
}

// validateStorageConfiguration should return an error because we are using a same device.
func TestValidateStorageConfigurationByUsingUsedDevice(t *testing.T) {
	oldStorage := map[string]interface{}{
		"type":    "device",
		"devices": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newStorage := map[string]interface{}{
		"type": "device",
		"devices": []string{
			"/opt/data01/xvd01", "/opt/data01/xvd02",
			"/opt/data01/xvd01", "/opt/data01/xvd03", // <- Using same device "/opt/data01/xvd01" twice
		},
	}

	err := validateStorageConfiguration(oldStorage, newStorage, "test-namespace")
	if err == nil {
		t.Fatalf("Got error while validating storage configuration: %v", err)
	}
}

// validateStorageConfiguration should return an error because we are deleting a device.
func TestValidateStorageConfigurationByDeletingDevice(t *testing.T) {
	oldStorage := map[string]interface{}{
		"type":    "device",
		"devices": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newStorage := map[string]interface{}{
		"type": "device",
		"devices": []string{
			"/opt/data01/xvd02", "/opt/data01/xvd03", "/opt/data01/xvd04", // <- Deleting device "/opt/data01/xvd01"
		},
	}

	err := validateStorageConfiguration(oldStorage, newStorage, "test-namespace")
	if err == nil {
		t.Fatalf("Got error while validating storage configuration: %v", err)
	}
}

//******************************************************************************
// 									Helper
//******************************************************************************

func buildEmptyNsConf() AerospikeConfigSpec {
	emptyConf := AerospikeConfigSpec{
		Value: map[string]interface{}{
			"namespaces":              []interface{}{},
			"tls-name":                "test-tls",
			"replication-factor":      2,
			"tls-authenticate-client": "test-auth-tls",
		},
	}
	return emptyConf
}

func addDeviceToPersistentNamesapce(namespace string, device string, conf *AerospikeConfigSpec) {
	config := conf.Value
	nsConfList := config["namespaces"].([]interface{})

	exists, index := namespaceAlreadyExists(nsConfList, namespace)

	if !exists {
		newNamespace := map[string]interface{}{
			"name": namespace,
			"storage-engine": map[string]interface{}{
				"type":    "device",
				"devices": []string{device},
			},
		}
		nsConfList = append(nsConfList, newNamespace)
	} else {
		nsConf := nsConfList[index].(map[string]interface{})
		storage := nsConf["storage-engine"].(map[string]interface{})
		devices := storage["devices"].([]string)
		devices = append(devices, device)
		storage["devices"] = devices
		nsConf["storage-engine"] = storage
		nsConfList[index] = nsConf
	}

	conf.Value["namespaces"] = nsConfList
}

func namespaceAlreadyExists(nsConfList []interface{}, namespace string) (bool, int) {
	if len(nsConfList) == 0 {
		return false, -1
	}

	for index, nsConfInterface := range nsConfList {
		ncConf := nsConfInterface.(map[string]interface{})
		if namespace == ncConf["name"] {
			return true, index
		}
	}

	return false, -1
}

func prepareNsConfigurations(oldNamespaceConf map[string][]string, newNamespaceConf map[string][]string) (AerospikeConfigSpec, AerospikeConfigSpec) {
	oldNamespaceConfig := buildEmptyNsConf()
	newNamespaceConfig := buildEmptyNsConf()

	for namespace, deviceList := range oldNamespaceConf {
		for _, device := range deviceList {
			addDeviceToPersistentNamesapce(namespace, device, &oldNamespaceConfig)
		}
	}

	for namespace, deviceList := range newNamespaceConf {
		for _, device := range deviceList {
			addDeviceToPersistentNamesapce(namespace, device, &newNamespaceConfig)
		}
	}

	return oldNamespaceConfig, newNamespaceConfig
}
