"use client";

import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import api from "@/lib/api";
import { useAuth } from "@/lib/auth-context";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Edit2, Sun, Compass } from "lucide-react";
import {
    Dialog,
    DialogContent,
    DialogHeader,
    DialogTitle,
    DialogFooter,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";

interface UnitListProps {
    type: "condo" | "hdb";
    id: string;
}

export function UnitList({ type, id }: UnitListProps) {
    const queryClient = useQueryClient();
    const { user } = useAuth();
    const [editingUnit, setEditingUnit] = useState<any>(null);
    const [isModalOpen, setIsModalOpen] = useState(false);

    const canEdit = user?.role === "agent" || user?.role === "admin";

    const { data: units, isLoading } = useQuery({
        queryKey: ["units", type, id],
        queryFn: async () => {
            const res = await api.get(`/directory/${type === "condo" ? "condos" : "hdbs"}/${id}/units`);
            return res.data;
        },
        enabled: !!id,
    });

    const updateMutation = useMutation({
        mutationFn: async (updatedData: any) => {
            const endpoint = type === "condo" 
                ? `/directory/condos/units/${editingUnit.unit_id}` 
                : `/directory/hdbs/units/${editingUnit.unit_id}`;
            const res = await api.patch(endpoint, updatedData);
            return res.data;
        },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["units", type, id] });
            setIsModalOpen(false);
            setEditingUnit(null);
        },
        onError: (error: any) => {
            const message = error.response?.data?.detail || "Failed to update unit detail. Ensure you are a verified agent.";
            alert(message);
        }
    });

    const handleEdit = (unit: any) => {
        if (!canEdit) return;
        setEditingUnit({ ...unit });
        setIsModalOpen(true);
    };

    const handleSave = () => {
        updateMutation.mutate({
            direction_facing: editingUnit.direction_facing,
            afternoon_sun: editingUnit.afternoon_sun,
            unique_unit_description: editingUnit.unique_unit_description,
        });
    };

    if (isLoading) return (
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
            {[1, 2, 3, 4, 5, 6].map(i => <div key={i} className="h-20 bg-muted animate-pulse rounded-lg" />)}
        </div>
    );

    return (
        <div className="space-y-6">
            <div className="flex justify-between items-center">
                <h3 className="text-xl font-semibold">Property Units Directory</h3>
                <Badge variant="outline" className="text-xs font-normal">
                    {units?.length || 0} Total Units
                </Badge>
            </div>

            <p className="text-sm text-muted-foreground italic">
                {canEdit 
                    ? "As a verified professional, you can contribute detailed attributes (facing, sun exposure) to the master directory."
                    : "The units below represent the master layout. Verified agents contribute granular data to ensure directory accuracy."
                }
            </p>

            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
                {units?.map((unit: any) => (
                    <Card key={unit.unit_id} className={`group hover:border-primary transition-colors hover:shadow-sm ${canEdit ? "cursor-default" : ""}`}>
                        <CardContent className="p-3">
                            <div className="flex justify-between items-start mb-2">
                                <span className="text-sm font-bold">#{unit.unit_number}</span>
                                {canEdit && (
                                    <Button 
                                        variant="ghost" 
                                        size="icon" 
                                        className="h-6 w-6 opacity-0 group-hover:opacity-100 transition-opacity"
                                        onClick={() => handleEdit(unit)}
                                    >
                                        <Edit2 className="h-3 w-3" />
                                    </Button>
                                )}
                            </div>
                            
                            <div className="space-y-1">
                                <div className="flex items-center text-[10px] text-muted-foreground">
                                    <Compass className="h-2.5 w-2.5 mr-1" />
                                    <span>{unit.direction_facing || "Unknown"}</span>
                                </div>
                                <div className="flex items-center text-[10px] text-muted-foreground">
                                    <Sun className="h-2.5 w-2.5 mr-1" />
                                    <span className={unit.afternoon_sun ? "text-orange-500" : ""}>
                                        {unit.afternoon_sun ? "Afternoon Sun" : "No Afternoon Sun"}
                                    </span>
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                ))}
            </div>

            {/* Edit Modal */}
            <Dialog open={isModalOpen} onOpenChange={setIsModalOpen}>
                <DialogContent>
                    <DialogHeader>
                        <DialogTitle>Update Master Directory Detail: #{editingUnit?.unit_number}</DialogTitle>
                    </DialogHeader>
                    
                    <div className="grid gap-4 py-4">
                        <div className="grid gap-2">
                            <Label htmlFor="facing">Physical Orientation (Facing)</Label>
                            <Select 
                                value={editingUnit?.direction_facing || ""} 
                                onValueChange={(val) => setEditingUnit({...editingUnit, direction_facing: val})}
                            >
                                <SelectTrigger>
                                    <SelectValue placeholder="Select Facing" />
                                </SelectTrigger>
                                <SelectContent>
                                    {["N", "S", "E", "W", "NE", "NW", "SE", "SW"].map(d => (
                                        <SelectItem key={d} value={d}>{d}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>

                        <div className="flex items-center justify-between">
                            <Label htmlFor="sun">Afternoon Sun Exposure?</Label>
                            <Switch 
                                id="sun" 
                                checked={editingUnit?.afternoon_sun || false}
                                onCheckedChange={(val: boolean) => setEditingUnit({...editingUnit, afternoon_sun: val})}
                            />
                        </div>

                        <div className="grid gap-2">
                            <Label htmlFor="desc">Specific Unit Characteristics</Label>
                            <Input 
                                id="desc" 
                                value={editingUnit?.unique_unit_description || ""}
                                onChange={(e) => setEditingUnit({...editingUnit, unique_unit_description: e.target.value})}
                                placeholder="e.g. Unblocked greenery view, premium stack"
                            />
                        </div>
                    </div>

                    <DialogFooter>
                        <Button variant="outline" onClick={() => setIsModalOpen(false)}>Cancel</Button>
                        <Button onClick={handleSave} disabled={updateMutation.isPending}>
                            {updateMutation.isPending ? "Updating Master Directory..." : "Contribute Data"}
                        </Button>
                    </DialogFooter>
                </DialogContent>
            </Dialog>
        </div>
    );
}
