/**
 * Enhanced Session Close Detector
 *
 * Usa multiplas estrategias para detectar fechamento de sessao de forma confiavel:
 * 1. pagehide - Mais confiavel que beforeunload
 * 2. visibilitychange - Detecta quando usuario muda de aba
 * 3. beforeunload - Fallback classico
 * 4. Heartbeat monitoring - Server-side fallback
 */

(function() {
    'use strict';

    const SESSION_ID = window.STREAMLIT_SESSION_ID || 'unknown';
    const CLOSE_ENDPOINT = '/close_session';  // Endpoint HTTP customizado

    let closeNotified = false;
    let lastHeartbeat = Date.now();
    let hiddenTime = null;

    /**
     * Notifica servidor sobre fechamento da sessao
     * Usa multiplas tecnicas para garantir entrega
     */
    function notifySessionClose(reason) {
        if (closeNotified) {
            console.log('[CloseDetector] Ja notificado, ignorando');
            return;
        }

        closeNotified = true;
        const timestamp = new Date().toISOString();

        console.log('[CloseDetector] Notificando fechamento:', reason);

        // Tentativa 1: navigator.sendBeacon() - Mais confiavel
        if (navigator.sendBeacon) {
            const data = JSON.stringify({
                session_id: SESSION_ID,
                reason: reason,
                timestamp: timestamp
            });

            try {
                const sent = navigator.sendBeacon(CLOSE_ENDPOINT, data);
                if (sent) {
                    console.log('[CloseDetector] Beacon enviado com sucesso');
                    return;
                }
            } catch (e) {
                console.warn('[CloseDetector] Beacon falhou:', e);
            }
        }

        // Tentativa 2: Fetch com keepalive
        try {
            fetch(CLOSE_ENDPOINT, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    session_id: SESSION_ID,
                    reason: reason,
                    timestamp: timestamp
                }),
                keepalive: true  // Garante que request complete mesmo apos pagina fechar
            }).catch(err => {
                console.warn('[CloseDetector] Fetch keepalive falhou:', err);
            });

            console.log('[CloseDetector] Fetch keepalive enviado');
        } catch (e) {
            console.error('[CloseDetector] Fetch com keepalive falhou:', e);
        }

        // Tentativa 3: LocalStorage como fallback
        // Se nenhuma tecnica acima funcionar, armazenar e enviar no proximo load
        try {
            localStorage.setItem('pending_session_close', JSON.stringify({
                session_id: SESSION_ID,
                reason: reason,
                timestamp: timestamp
            }));
            console.log('[CloseDetector] Estado salvo em localStorage');
        } catch (e) {
            console.warn('[CloseDetector] LocalStorage falhou:', e);
        }

        // Tentativa 4: Notificar via Streamlit component (assíncrono, pode falhar)
        try {
            if (window.parent && window.parent.document) {
                const event = new CustomEvent('streamlit:setComponentValue', {
                    detail: {
                        value: {
                            event: 'session_close',
                            session_id: SESSION_ID,
                            reason: reason,
                            timestamp: timestamp
                        }
                    }
                });
                window.parent.document.dispatchEvent(event);
            }
        } catch (e) {
            console.warn('[CloseDetector] Streamlit event falhou:', e);
        }
    }

    /**
     * Event Listener: pagehide
     * Mais confiavel que beforeunload, dispara quando pagina e descartada da memoria
     */
    window.addEventListener('pagehide', function(event) {
        console.log('[CloseDetector] pagehide detectado');
        notifySessionClose('pagehide');
    }, { capture: true });

    /**
     * Event Listener: beforeunload
     * Classico, mas menos confiavel que pagehide
     */
    window.addEventListener('beforeunload', function(event) {
        console.log('[CloseDetector] beforeunload detectado');
        notifySessionClose('beforeunload');
        // NAO mostrar dialogo de confirmacao
        // return undefined;
    });

    /**
     * Event Listener: unload
     * Ultima tentativa antes da pagina fechar
     */
    window.addEventListener('unload', function() {
        console.log('[CloseDetector] unload detectado');
        notifySessionClose('unload');
    });

    /**
     * Event Listener: visibilitychange
     * Detecta quando usuario muda de aba ou minimiza janela
     */
    document.addEventListener('visibilitychange', function() {
        if (document.hidden) {
            hiddenTime = Date.now();
            console.log('[CloseDetector] Pagina oculta');

            // Se usuario ficar mais de 5 minutos com aba oculta, considerar fechamento
            setTimeout(function() {
                if (document.hidden && hiddenTime && (Date.now() - hiddenTime > 5 * 60 * 1000)) {
                    console.log('[CloseDetector] Pagina oculta por muito tempo, notificando');
                    notifySessionClose('long_hidden');
                }
            }, 5 * 60 * 1000);
        } else {
            hiddenTime = null;
            console.log('[CloseDetector] Pagina visivel');
        }
    });

    /**
     * Verificar localStorage ao carregar pagina
     * Processar fechamentos pendentes da sessao anterior
     */
    window.addEventListener('load', function() {
        try {
            const pendingClose = localStorage.getItem('pending_session_close');
            if (pendingClose) {
                const data = JSON.parse(pendingClose);
                console.log('[CloseDetector] Processando fechamento pendente:', data);

                // Enviar fechamento pendente
                fetch(CLOSE_ENDPOINT, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(data)
                }).then(() => {
                    localStorage.removeItem('pending_session_close');
                    console.log('[CloseDetector] Fechamento pendente processado');
                }).catch(err => {
                    console.error('[CloseDetector] Erro ao processar fechamento pendente:', err);
                });
            }
        } catch (e) {
            console.error('[CloseDetector] Erro ao verificar localStorage:', e);
        }
    });

    /**
     * Heartbeat monitoring
     * Atualizar timestamp de ultimo heartbeat
     */
    function updateHeartbeat() {
        lastHeartbeat = Date.now();
    }

    // Listener para eventos de atividade
    ['mousedown', 'mousemove', 'keypress', 'scroll', 'touchstart', 'click'].forEach(function(eventType) {
        document.addEventListener(eventType, updateHeartbeat, { passive: true, capture: true });
    });

    console.log('[CloseDetector] Inicializado para session:', SESSION_ID);
})();
