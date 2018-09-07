package store

// GetAPIAddr returns the apiAddr for specified node
func (kv *KV) GetAPIAddr(nodeID string) string {
	v, _ := kv.meta.Load(nodeID)
	s, _ := v.(string)
	return s
}

// GetNodes returns all nodes for service
func (kv *KV) GetNodes(service string) []Node {
	val, ok := kv.data.Load(service)
	if !ok {
		return nil
	}
	return val.(*NodeSet).Members()
}
