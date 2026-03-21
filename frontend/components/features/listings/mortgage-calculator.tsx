"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Calculator } from "lucide-react";

interface MortgageCalculatorProps {
  propertyPrice: number;
}

export function MortgageCalculator({ propertyPrice }: MortgageCalculatorProps) {
  const [downpaymentPercent, setDownpaymentPercent] = useState([25]);
  const [interestRate, setInterestRate] = useState([3.5]);
  const [tenureYears, setTenureYears] = useState([30]);
  const [monthlyPayment, setMonthlyPayment] = useState(0);

  useEffect(() => {
    if (!propertyPrice) return;
    
    const p = propertyPrice - (propertyPrice * (downpaymentPercent[0] / 100));
    const r = interestRate[0] / 100 / 12;
    const n = tenureYears[0] * 12;
    
    if (r === 0) {
      setMonthlyPayment(p / n);
      return;
    }
    
    const payment = (p * r * Math.pow(1 + r, n)) / (Math.pow(1 + r, n) - 1);
    setMonthlyPayment(payment);
  }, [propertyPrice, downpaymentPercent, interestRate, tenureYears]);

  if (!propertyPrice) return null;

  const downpaymentAmount = propertyPrice * (downpaymentPercent[0] / 100);
  const loanAmount = propertyPrice - downpaymentAmount;

  return (
    <Card className="mt-8 border-violet-100 shadow-sm overflow-hidden">
      <CardHeader className="bg-violet-50/50 pb-4 border-b border-violet-100">
        <CardTitle className="flex items-center gap-2 text-violet-900 font-bold">
          <Calculator className="h-5 w-5 text-violet-600" />
          Affordability Calculator
        </CardTitle>
      </CardHeader>
      <CardContent className="p-6">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          <div className="space-y-8 py-2">
            <div className="space-y-4">
              <div className="flex justify-between">
                <span className="text-sm font-medium text-gray-700">Downpayment</span>
                <span className="text-sm font-bold text-violet-700">{downpaymentPercent[0]}% (${downpaymentAmount.toLocaleString()})</span>
              </div>
              <input type="range" value={downpaymentPercent[0]} onChange={(e) => setDownpaymentPercent([Number(e.target.value)])} max={80} min={5} step={5} className="w-full h-2 bg-violet-200 rounded-lg appearance-none cursor-pointer accent-violet-600" />
            </div>

            <div className="space-y-4">
              <div className="flex justify-between">
                <span className="text-sm font-medium text-gray-700">Interest Rate (p.a.)</span>
                <span className="text-sm font-bold text-violet-700">{interestRate[0]}%</span>
              </div>
              <input type="range" value={interestRate[0]} onChange={(e) => setInterestRate([Number(e.target.value)])} max={7} min={1} step={0.1} className="w-full h-2 bg-violet-200 rounded-lg appearance-none cursor-pointer accent-violet-600" />
            </div>

            <div className="space-y-4">
              <div className="flex justify-between">
                <span className="text-sm font-medium text-gray-700">Loan Tenure</span>
                <span className="text-sm font-bold text-violet-700">{tenureYears[0]} Years</span>
              </div>
              <input type="range" value={tenureYears[0]} onChange={(e) => setTenureYears([Number(e.target.value)])} max={35} min={5} step={1} className="w-full h-2 bg-violet-200 rounded-lg appearance-none cursor-pointer accent-violet-600" />
            </div>
          </div>
          
          <div className="bg-gradient-to-br from-violet-50 to-indigo-50 rounded-xl p-6 flex flex-col justify-center items-center h-full border border-violet-100 shadow-inner">
            <div className="text-sm text-violet-600 font-bold mb-3 uppercase tracking-wider">Est. Monthly Instalment</div>
            <div className="text-4xl md:text-5xl font-black text-violet-950">${Math.round(monthlyPayment).toLocaleString('en-SG')}</div>
            <div className="mt-4 text-xs text-violet-500 font-medium bg-white/60 px-3 py-1.5 rounded-full border border-violet-100">
              Based on a loan of ${loanAmount.toLocaleString()}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
