import React, { useState } from 'react';
import './App.css'; // You can style this later!

function App() {
  const [messages, setMessages] = useState([
    { sender: 'agent', text: 'Welcome to the Secure AI Banking Vault. How can I assist you today?' }
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);

  const sendMessage = async () => {
    if (!input.trim()) return;

    // 1. Add user message to UI
    const newMessages = [...messages, { sender: 'user', text: input }];
    setMessages(newMessages);
    setInput('');
    setLoading(true);

    try {
      // 2. Send to our new Python Agent API
      const response = await fetch('http://127.0.0.1:8001/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: input, session_id: 'react_user_1' })
      });
      
      const data = await response.json();
      
      // 3. Add AI response to UI
      setMessages([...newMessages, { sender: 'agent', text: data.response }]);
    } catch (error) {
      setMessages([...newMessages, { sender: 'agent', text: "Error connecting to the Bank Agent." }]);
    }
    setLoading(false);
  };

  return (
    <div style={{ maxWidth: '600px', margin: '50px auto', fontFamily: 'Arial' }}>
      <h2>🏦 Secure AI Banking Agent</h2>
      
      <div style={{ border: '1px solid #ccc', padding: '20px', height: '400px', overflowY: 'auto', borderRadius: '8px', marginBottom: '20px' }}>
        {messages.map((msg, index) => (
          <div key={index} style={{ textAlign: msg.sender === 'user' ? 'right' : 'left', margin: '10px 0' }}>
            <span style={{ 
              background: msg.sender === 'user' ? '#007bff' : '#f1f1f1', 
              color: msg.sender === 'user' ? 'white' : 'black',
              padding: '10px 15px', 
              borderRadius: '20px', 
              display: 'inline-block' 
            }}>
              {msg.text}
            </span>
          </div>
        ))}
        {loading && <div style={{ textAlign: 'left', color: 'gray' }}>Agent is thinking...</div>}
      </div>

      <div style={{ display: 'flex' }}>
        <input 
          style={{ flex: 1, padding: '10px', fontSize: '16px', borderRadius: '4px', border: '1px solid #ccc' }}
          value={input} 
          onChange={(e) => setInput(e.target.value)}
          onKeyPress={(e) => e.key === 'Enter' && sendMessage()}
          placeholder="Ask about your balance, transfer funds, or policy..."
        />
        <button 
          style={{ padding: '10px 20px', marginLeft: '10px', fontSize: '16px', cursor: 'pointer', background: '#28a745', color: 'white', border: 'none', borderRadius: '4px' }}
          onClick={sendMessage}
        >
          Send
        </button>
      </div>
    </div>
  );
}

export default App;