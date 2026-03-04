"use client";

import { useState, useRef, useEffect } from "react";
import api from "@/lib/api";
import { Sparkles, Send, Loader2, MessageCircle, ChevronDown, ChevronUp } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

interface PropertyChatPanelProps {
  listingId: number;
  valuation?: object | null;  // ValuationResult from valuation-panel, passed through
}

const STARTER_PROMPTS = [
  "Is this fairly priced?",
  "What are the pros and cons?",
  "How does location affect the value?",
  "What's the rental yield potential?",
];

export function PropertyChatPanel({ listingId, valuation }: PropertyChatPanelProps) {
  const [open, setOpen] = useState(false);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Scroll to bottom when new messages arrive
  useEffect(() => {
    if (open) {
      bottomRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages, open]);

  // Focus input when panel opens
  useEffect(() => {
    if (open) {
      setTimeout(() => inputRef.current?.focus(), 100);
    }
  }, [open]);

  const sendMessage = async (text: string) => {
    if (!text.trim() || loading) return;

    const userMsg: ChatMessage = { role: "user", content: text.trim() };
    const newMessages = [...messages, userMsg];
    setMessages(newMessages);
    setInput("");
    setLoading(true);

    try {
      const { data } = await api.post(`/listings/${listingId}/chat`, {
        messages: newMessages,
        valuation: valuation ?? null,
      });
      setMessages([...newMessages, { role: "assistant", content: data.reply }]);
    } catch {
      setMessages([
        ...newMessages,
        { role: "assistant", content: "Sorry, I couldn't process that request. Please try again." },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    sendMessage(input);
  };

  return (
    <div className="rounded-xl border bg-gradient-to-br from-violet-50 to-white overflow-hidden">
      {/* Toggle header */}
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between px-5 py-4 hover:bg-violet-50/80 transition-colors"
      >
        <div className="flex items-center gap-2">
          <span className="flex items-center justify-center rounded-lg bg-violet-600 p-1.5">
            <MessageCircle className="h-4 w-4 text-white" />
          </span>
          <div className="text-left">
            <div className="font-semibold text-base text-violet-900 flex items-center gap-1.5">
              Ask AI About This Property
              <Sparkles className="h-3.5 w-3.5 text-violet-400" />
            </div>
            <div className="text-xs text-muted-foreground">
              Get instant answers about pricing, location & investment potential
            </div>
          </div>
        </div>
        {open ? (
          <ChevronUp className="h-4 w-4 text-muted-foreground shrink-0" />
        ) : (
          <ChevronDown className="h-4 w-4 text-muted-foreground shrink-0" />
        )}
      </button>

      {open && (
        <div className="border-t">
          {/* Starter prompts — only shown when no messages yet */}
          {messages.length === 0 && (
            <div className="px-4 pt-3 pb-2 flex flex-wrap gap-2">
              {STARTER_PROMPTS.map((p) => (
                <button
                  key={p}
                  onClick={() => sendMessage(p)}
                  className="text-xs px-3 py-1.5 rounded-full border border-violet-200 bg-violet-50 text-violet-700 hover:bg-violet-100 transition-colors"
                >
                  {p}
                </button>
              ))}
            </div>
          )}

          {/* Message list */}
          {messages.length > 0 && (
            <div className="px-4 py-3 space-y-3 max-h-72 overflow-y-auto">
              {messages.map((msg, i) => (
                <div
                  key={i}
                  className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
                >
                  {msg.role === "assistant" && (
                    <div className="flex items-start gap-2 max-w-[85%]">
                      <div className="shrink-0 mt-0.5 flex items-center justify-center rounded-full bg-violet-600 h-6 w-6">
                        <Sparkles className="h-3 w-3 text-white" />
                      </div>
                      <div className="rounded-2xl rounded-tl-sm bg-white border border-violet-100 px-3 py-2 text-sm text-gray-800 shadow-sm leading-relaxed">
                        {msg.content}
                      </div>
                    </div>
                  )}
                  {msg.role === "user" && (
                    <div className="max-w-[80%] rounded-2xl rounded-tr-sm bg-violet-600 px-3 py-2 text-sm text-white shadow-sm">
                      {msg.content}
                    </div>
                  )}
                </div>
              ))}
              {loading && (
                <div className="flex justify-start">
                  <div className="flex items-center gap-2">
                    <div className="shrink-0 flex items-center justify-center rounded-full bg-violet-600 h-6 w-6">
                      <Sparkles className="h-3 w-3 text-white" />
                    </div>
                    <div className="rounded-2xl rounded-tl-sm bg-white border border-violet-100 px-3 py-2 shadow-sm">
                      <Loader2 className="h-4 w-4 animate-spin text-violet-500" />
                    </div>
                  </div>
                </div>
              )}
              <div ref={bottomRef} />
            </div>
          )}

          {/* Input bar */}
          <form onSubmit={handleSubmit} className="flex gap-2 px-4 py-3 border-t bg-white/60">
            <Input
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Ask anything about this property…"
              className="flex-1 h-9 text-sm rounded-xl border-gray-200 focus-visible:ring-violet-500/50"
              disabled={loading}
            />
            <Button
              type="submit"
              size="icon"
              className="h-9 w-9 rounded-xl bg-violet-600 hover:bg-violet-700 shrink-0"
              disabled={!input.trim() || loading}
            >
              {loading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Send className="h-4 w-4" />
              )}
            </Button>
          </form>
        </div>
      )}
    </div>
  );
}
