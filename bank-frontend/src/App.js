import React, { useState, useRef, useEffect } from 'react';
import './App.css';

function App() {
  const [messages, setMessages] = useState([
    { sender: 'agent', text: 'Welcome to the Secure AI Banking Vault. How can I assist you today?' }
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef(null);

  // Auto-scroll to the bottom when a new message is added
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const sendMessage = async () => {
    if (!input.trim()) return;

    const newMessages = [...messages, { sender: 'user', text: input }];
    setMessages(newMessages);
    setInput('');
    setLoading(true);

    try {
      const response = await fetch('http://127.0.0.1:8001/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: input, session_id: 'react_user_1' })
      });
      
      const data = await response.json();
      setMessages([...newMessages, { sender: 'agent', text: data.response }]);
    } catch (error) {
      setMessages([...newMessages, { sender: 'agent', text: "Error connecting to the Bank Agent." }]);
    }
    setLoading(false);
  };

  return (
    <div className="app-container">
      <header className="app-header">
        <h2 className="gradient-text">✨ Secure AI Banking</h2>
      </header>
      
      <div className="chat-container">
        {messages.map((msg, index) => (
          <div key={index} className={`message-wrapper ${msg.sender}`}>
            {msg.sender === 'agent' && <div className="avatar agent-avatar">✨</div>}
            
            <div className={`message-bubble ${msg.sender}`}>
              {msg.text}
            </div>

            {msg.sender === 'user' && <div className="avatar user-avatar">👤</div>}
          </div>
        ))}
        
        {/* Animated Loading Dots */}
        {loading && (
          <div className="message-wrapper agent">
            <div className="avatar agent-avatar">✨</div>
            <div className="message-bubble agent typing-indicator">
              <span></span><span></span><span></span>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      <div className="input-container">
        <div className="input-box">
          <input 
            value={input} 
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && sendMessage()}
            placeholder="Ask about your balance, transfer funds, or bank policy..."
          />
          <button onClick={sendMessage} disabled={!input.trim() || loading}>
            {/* SVG Send Icon */}
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="22" y1="2" x2="11" y2="13"></line>
              <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
            </svg>
          </button>
        </div>
      </div>
    </div>
  );
}

export default App;