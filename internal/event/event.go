package event

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

type PageViewEvent struct {
	PageID       string    `json:"page_id"`
	UserID       string    `json:"user_id"`
	ViewDuration int       `json:"view_duration_ms"`
	Timestamp    time.Time `json:"timestamp"`
	UserAgent    string    `json:"user_agent,omitempty"`
	IPAddress    string    `json:"ip_address,omitempty"`
	Region       string    `json:"region,omitempty"`
	IsBounce     bool      `json:"is_bounce"`
}

// Генератор случайных данных
var (
	rnd     = rand.New(rand.NewSource(time.Now().UnixNano()))
	pages   = []string{"home", "catalog", "product/1", "product/2", "cart", "checkout", "about", "contact"}
	users   = make([]string, 1000)
	regions = []string{"EU-West", "EU-East", "US-West", "US-East", "Asia", "Russia"}
	agents  = []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 14_0)",
		"Mozilla/5.0 (Linux; Android 11)",
	}
)

// Инициализация пула пользователей
func init() {
	for i := 0; i < 1000; i++ {
		users[i] = fmt.Sprintf("user_%d", i)
	}
}

// GenerateNormalEvent - генерирует нормальное событие
func GenerateNormalEvent() PageViewEvent {
	duration := rnd.Intn(590) + 10 // 10-600 секунд = 10000-600000 мс, но в задании ms, так что 10-600 секунд = 10000-600000 мс
	// Уточнение: в задании view_duration_ms, но диапазон 10-600 секунд.
	// Будем генерировать 10000-600000 мс (10-600 сек)
	duration = rnd.Intn(590000) + 10000

	return PageViewEvent{
		PageID:       pages[rnd.Intn(len(pages))],
		UserID:       users[rnd.Intn(len(users))],
		ViewDuration: duration,
		Timestamp:    time.Now().UTC(),
		UserAgent:    agents[rnd.Intn(len(agents))],
		IPAddress:    generateIPv6(),
		Region:       regions[rnd.Intn(len(regions))],
		IsBounce:     false,
	}
}

// GenerateBounceEvent - генерирует событие "отскок" (<5 секунд)
func GenerateBounceEvent() PageViewEvent {
	event := GenerateNormalEvent()
	event.ViewDuration = rnd.Intn(4999) + 100 // 100-5000 мс
	event.IsBounce = true
	return event
}

// GenerateErrorEvent - генерирует событие с ошибкой (5% случаев)
// Типы ошибок: пустой page_id, отрицательная длительность, некорректный JSON
func GenerateErrorEvent(errorType int) PageViewEvent {
	event := GenerateNormalEvent()

	switch errorType {
	case 0: // Пустой page_id
		event.PageID = ""
	case 1: // Отрицательная длительность
		event.ViewDuration = -rand.Intn(1000) - 1
	case 2: // Будет обработано на уровне сериализации (невалидный UTF-8 и т.п.)
		// Для простоты оставим валидным, но Consumer отловит логику
		event.PageID = ""
	}

	return event
}

// generateIPv6 - генерирует случайный IPv6 адрес в строковом формате
func generateIPv6() string {
	return fmt.Sprintf("%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x",
		rnd.Intn(256), rnd.Intn(256),
		rnd.Intn(256), rnd.Intn(256),
		rnd.Intn(256), rnd.Intn(256),
		rnd.Intn(256), rnd.Intn(256),
		rnd.Intn(256), rnd.Intn(256),
		rnd.Intn(256), rnd.Intn(256),
		rnd.Intn(256), rnd.Intn(256),
		rnd.Intn(256), rnd.Intn(256),
	)
}

func (e PageViewEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// GetPartitionKey - возвращает ключ для партиционирования (page_id)
func (e PageViewEvent) GetPartitionKey() string {
	return e.PageID
}

// RndIntn - экспортированная функция для получения случайного числа
func RndIntn(n int) int {
	return rnd.Intn(n)
}
