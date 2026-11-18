package procurementhttp

import "github.com/gin-gonic/gin"

func RegisterAdminRoutes(admin gin.IRoutes, handler *AdminHandler) {
	if admin == nil || handler == nil {
		panic("procurement admin routes: required dependency is nil")
	}
	admin.GET("/procurement-orders", handler.GetProcurementOrders)
	admin.GET("/procurement-orders/stats", handler.GetProcurementOrderStats)
	admin.GET("/procurement-orders/:id", handler.GetProcurementOrder)
	admin.GET("/procurement-orders/:id/upstream-payload/download", handler.DownloadProcurementUpstreamPayload)
	admin.POST("/procurement-orders/:id/retry", handler.RetryProcurementOrder)
	admin.POST("/procurement-orders/:id/resolve-unknown", handler.ResolveUnknownProcurementOrder)
	admin.POST("/procurement-orders/:id/authorize-unknown-retry", handler.AuthorizeUnknownProcurementRetry)
	admin.GET("/procurement-trade-response-journal", handler.ListTradeResponseJournal)
	admin.GET("/procurement-trade-evidence-conflicts", handler.ListTradeEvidenceConflicts)
	admin.POST("/procurement-trade-evidence-conflicts/:id/resolve", handler.ResolveTradeEvidenceConflict)
	admin.POST("/procurement-orders/:id/reconcile-trade-response", handler.RecordTradeResponseReconciliation)
	admin.POST("/procurement-orders/:id/cancel", handler.CancelProcurementOrder)
}
