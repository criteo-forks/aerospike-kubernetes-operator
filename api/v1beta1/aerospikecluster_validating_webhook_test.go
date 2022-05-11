package v1beta1

import (
	"testing"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var aslog = logf.Log.WithName("Test validateNsConfUpdate")

// validateNsConfUpdate should return nil because we are adding a new namespace with unused devices
func TestAddPesistentNamespaceWithNotUsedDevices(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
	}
	newNsConf := map[string][]string{
		"namespace-0":   {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace": {"/dev/nvme3", "/dev/nvme4"},
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
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
	}
	newNsConf := map[string][]string{
		"namespace-0":   {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace": {"/dev/nvme1", "/dev/nvme4"}, // <- Using same device "/dev/nvme1" as namespace-0
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
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
	}
	newNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
		"new-namespace-1": {"/dev/nvme5", "/dev/nvme6"},
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
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
	}
	newNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
		"new-namespace-1": {"/dev/nvme3", "/dev/nvme6"}, // <- Using same device "/dev/nvme3" as new-namespace-0
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
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {
			"/dev/nvme3", "/dev/nvme4",
			"/dev/nvme5", "/dev/nvme6",
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
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {
			"/dev/nvme3", "/dev/nvme4",
			"/dev/nvme1", "/dev/nvme6", // <- Using same device "/dev/nvme1" as namespace-0
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
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {
			"/dev/nvme3", "/dev/nvme4",
			"/dev/nvme3", "/dev/nvme6", // <- Using same device "/dev/nvme3" as new-namespace-0
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
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
		"new-namespace-1": {"/dev/nvme7", "/dev/nvme8"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {
			"/dev/nvme3", "/dev/nvme4",
			"/dev/nvme5", "/dev/nvme6",
		},
		"new-namespace-1": {
			"/dev/nvme7", "/dev/nvme8",
			"/dev/nvme9", "/dev/nvme10",
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
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
		"new-namespace-1": {"/dev/nvme7", "/dev/nvme8"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {
			"/dev/nvme3", "/dev/nvme4",
			"/dev/nvme5", "/dev/nvme6",
		},
		"new-namespace-1": {
			"/dev/nvme7", "/dev/nvme8",
			"/dev/nvme5", "/dev/nvme10", // <- Using same device "/dev/nvme5" as new-namespace-0
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	}
}

// validateStorageConfiguration should return nil because we are adding unused devices and keeping the same type.
func TestValidateStorageEngineConfiguration(t *testing.T) {
	oldStorage := map[string]interface{}{
		"type":    "device",
		"devices": []string{"/dev/nvme1", "/dev/nvme2"},
	}
	newStorage := map[string]interface{}{
		"type": "device",
		"devices": []string{
			"/dev/nvme1", "/dev/nvme2",
			"/dev/nvme3", "/dev/nvme4",
		},
	}

	err := validateStorageEngineConfiguration(oldStorage, newStorage, "test-namespace")
	if err != nil {
		t.Fatalf("Got error while validating storage configuration: %v", err)
	}
}

// validateStorageConfiguration should return an error because we are changing the type of the storage.
func TestValidateStorageEngineConfigurationByChangingStorageType(t *testing.T) {
	oldStorage := map[string]interface{}{
		"type":    "device",
		"devices": []string{"/dev/nvme1", "/dev/nvme2"},
	}
	newStorage := map[string]interface{}{
		"type":    "memory",
		"devices": []string{},
	}

	err := validateStorageEngineConfiguration(oldStorage, newStorage, "test-namespace")
	if err == nil {
		t.Fatalf("Got error while validating storage configuration: %v", err)
	}
}

// validateStorageConfiguration should return an error because we are using a same device.
func TestValidateStorageEngineConfigurationByUsingUsedDevice(t *testing.T) {
	oldStorage := map[string]interface{}{
		"type":    "device",
		"devices": []string{"/dev/nvme1", "/dev/nvme2"},
	}
	newStorage := map[string]interface{}{
		"type": "device",
		"devices": []string{
			"/dev/nvme1", "/dev/nvme2",
			"/dev/nvme1", "/dev/nvme3", // <- Using same device "/dev/nvme1" twice
		},
	}

	err := validateStorageEngineConfiguration(oldStorage, newStorage, "test-namespace")
	if err == nil {
		t.Fatalf("Got error while validating storage configuration: %v", err)
	}
}

// validateStorageConfiguration should return an error because we are deleting a device.
func TestValidateStorageEngineConfigurationByDeletingDevice(t *testing.T) {
	oldStorage := map[string]interface{}{
		"type":    "device",
		"devices": []string{"/dev/nvme1", "/dev/nvme2"},
	}
	newStorage := map[string]interface{}{
		"type": "device",
		"devices": []string{
			"/dev/nvme2", "/dev/nvme3", "/dev/nvme4", // <- Deleting device "/dev/nvme1"
		},
	}

	err := validateStorageEngineConfiguration(oldStorage, newStorage, "test-namespace")
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
