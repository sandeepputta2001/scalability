package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"

	"github.com/distributed-ecommerce/internal/middleware"
	"github.com/distributed-ecommerce/internal/models"
	"github.com/distributed-ecommerce/internal/repository"
)

type AuthHandler struct {
	userRepo  *repository.UserRepository
	jwtSecret string
	jwtExpiry time.Duration
	log       *zap.Logger
}

func NewAuthHandler(userRepo *repository.UserRepository, secret string, expiryHours int, log *zap.Logger) *AuthHandler {
	return &AuthHandler{
		userRepo:  userRepo,
		jwtSecret: secret,
		jwtExpiry: time.Duration(expiryHours) * time.Hour,
		log:       log,
	}
}

// Register godoc
// POST /api/v1/auth/register
func (h *AuthHandler) Register(c *gin.Context) {
	var req models.RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
		return
	}

	user := &models.User{
		Email:        req.Email,
		PasswordHash: string(hash),
		Name:         req.Name,
	}

	if err := h.userRepo.Create(c.Request.Context(), user); err != nil {
		h.log.Error("register failed", zap.Error(err))
		c.JSON(http.StatusConflict, gin.H{"error": "email already exists"})
		return
	}

	token, err := h.generateToken(user)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "token generation failed"})
		return
	}

	c.JSON(http.StatusCreated, models.LoginResponse{Token: token, User: user})
}

// Login godoc
// POST /api/v1/auth/login
func (h *AuthHandler) Login(c *gin.Context) {
	var req models.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	user, err := h.userRepo.GetByEmail(c.Request.Context(), req.Email)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	token, err := h.generateToken(user)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "token generation failed"})
		return
	}

	c.JSON(http.StatusOK, models.LoginResponse{Token: token, User: user})
}

func (h *AuthHandler) generateToken(user *models.User) (string, error) {
	claims := middleware.Claims{
		UserID:   user.ID.String(),
		ShardKey: user.ShardKey,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(h.jwtExpiry)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(h.jwtSecret))
}
