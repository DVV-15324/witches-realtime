package hub

func (h *Hub) Run() {
	// Subscribe Redis (broker)
	if h.broker != nil {
		brokerCh, err := h.broker.Subscribe(h.ctx)
		if err != nil {
			h.logger.Error("broker subscribe error", "err", err)
		} else {
			go func() {
				for msg := range brokerCh {
					select {
					case h.broadcast <- msg:
					default:
						h.logger.Error("broker broadcast channel full")
					}
				}
			}()
		}
	}

	for {
		select {
		case <-h.ctx.Done():
			h.logger.Info("hub stopping")
			return

		case client := <-h.register:
			h.mu.Lock()
			h.clients[client.GetID()] = client
			h.mu.Unlock()

			h.logger.Info("client connected", "id", client.GetID())
			if h.OnConnect != nil {
				h.OnConnect(client)
			}

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.GetID()]; ok {
				delete(h.clients, client.GetID())
				// Remove from all rooms
				for room, members := range h.rooms {
					if _, ok := members[client.GetID()]; ok {
						delete(members, client.GetID())
						if len(members) == 0 {
							delete(h.rooms, room)
						}
					}
				}
				client.Close()
			}
			h.mu.Unlock()

			h.logger.Info("client disconnected", "id", client.GetID())
			if h.OnDisconnect != nil {
				h.OnDisconnect(client)
			}

		// XỬ LÝ LOCAL MESSAGE NGAY TRONG RUN
		case lm := <-h.localMessage:
			h.handleLocalMessage(lm)

		case msg := <-h.broadcast:
			h.handleBrokerEvent(msg)
		}
	}
}
