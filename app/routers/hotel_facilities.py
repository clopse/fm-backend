// FILE: src/components/hotels/HotelStructuralTab.tsx
'use client';

import { StructuralInfo } from '@/types/hotelTypes';

interface HotelStructuralTabProps {
  structural: StructuralInfo;
  isEditing: boolean;
  onUpdate: (key: string, value: any) => void;
}

export default function HotelStructuralTab({ structural, isEditing, onUpdate }: HotelStructuralTabProps) {
  const renderField = (
    key: string, 
    value: any, 
    label: string, 
    type: 'number' | 'text' | 'select' = 'number',
    options?: string[],
    unit?: string,
    description?: string
  ) => (
    <div key={key} className="bg-white rounded-lg p-4 border border-gray-200">
      <label className="block text-sm font-medium text-gray-700 mb-2">
        {label} {unit && <span className="text-gray-500">({unit})</span>}
      </label>
      {type === 'select' ? (
        <select
          value={value || ''}
          onChange={(e) => onUpdate(key, e.target.value)}
          disabled={!isEditing}
          className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:bg-gray-50 disabled:text-gray-500"
        >
          <option value="">Select {label.toLowerCase()}</option>
          {options?.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      ) : (
        <input
          type={type}
          value={value || (type === 'number' ? 0 : '')}
          onChange={(e) => onUpdate(key, type === 'number' ? (parseInt(e.target.value) || 0) : e.target.value)}
          disabled={!isEditing}
          className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:bg-gray-50 disabled:text-gray-500"
          min={type === 'number' ? "0" : undefined}
          placeholder={type === 'number' ? '0' : `Enter ${label.toLowerCase()}`}
        />
      )}
      {description && (
        <p className="mt-2 text-xs text-gray-500">{description}</p>
      )}
    </div>
  );

  const fieldLabels: Record<string, { 
    label: string; 
    type: 'number' | 'text' | 'select';
    unit?: string;
    options?: string[];
    description?: string;
  }> = {
    floors: { 
      label: 'Number of Storeys', 
      type: 'number',
      description: 'Total storeys including ground floor'
    },
    basements: { 
      label: 'Number of Basements', 
      type: 'number',
      description: 'Below-ground levels'
    },
    totalRooms: { 
      label: 'Total Guest Rooms', 
      type: 'number',
      description: 'All guest accommodation rooms'
    },
    suites: { 
      label: 'Number of Suites', 
      type: 'number',
      description: 'Multi-room guest accommodations'
    },
    yearBuilt: { 
      label: 'Year Built', 
      type: 'number',
      description: 'Original construction year'
    },
    constructionYear: { 
      label: 'Construction Year', 
      type: 'number',
      description: 'Year of construction (alternative field)'
    },
    lastMajorRenovation: { 
      label: 'Last Major Renovation', 
      type: 'number',
      description: 'Year of most recent major renovation (optional)'
    },
    totalSquareMetres: { 
      label: 'Total Floor Area', 
      type: 'number',
      unit: 'm²',
      description: 'Total gross floor area in square metres'
    },
    buildingHeightMetres: { 
      label: 'Building Height', 
      type: 'number',
      unit: 'm',
      description: 'Height from ground level to highest point'
    }
  };

  // Group fields for better organization
  const fieldGroups = [
    {
      title: 'Basic Information',
      fields: ['floors', 'basements', 'totalRooms', 'suites']
    },
    {
      title: 'Building History',
      fields: ['yearBuilt', 'constructionYear', 'lastMajorRenovation']
    },
    {
      title: 'Physical Dimensions',
      fields: ['totalSquareMetres', 'buildingHeightMetres']
    }
  ];

  // Calculate some useful metrics
  const roomsPerFloor = structural.floors > 0 ? Math.round((structural.totalRooms || 0) / structural.floors) : 0;
  const buildingAge = new Date().getFullYear() - (structural.yearBuilt || 0);
  const areaPerRoom = (structural.totalSquareMetres && structural.totalRooms) 
    ? Math.round((structural.totalSquareMetres || 0) / (structural.totalRooms || 1)) 
    : 0;

  return (
    <div className="space-y-8">
      <div>
        <h3 className="text-lg font-semibold text-gray-900 mb-2">Building Information</h3>
        <p className="text-sm text-gray-600">
          Structural and construction details for compliance and safety planning.
        </p>
      </div>

      {fieldGroups.map((group) => (
        <div key={group.title} className="bg-gray-50 rounded-lg p-6">
          <h4 className="text-md font-medium text-gray-800 mb-4">{group.title}</h4>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {group.fields.map((key) => {
              const field = fieldLabels[key];
              if (!field) return null;
              return renderField(
                key, 
                structural[key as keyof StructuralInfo], 
                field.label, 
                field.type,
                field.options,
                field.unit,
                field.description
              );
            })}
          </div>
        </div>
      ))}

      {/* Building Metrics Summary */}
      <div className="bg-blue-50 rounded-lg p-6">
        <h4 className="text-md font-medium text-blue-900 mb-3">Building Metrics</h4>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <div className="text-center">
            <div className="text-2xl font-bold text-blue-600">
              {roomsPerFloor}
            </div>
            <div className="text-blue-700">Rooms per Storey</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-blue-600">
              {buildingAge > 0 ? buildingAge : '—'}
            </div>
            <div className="text-blue-700">Building Age (years)</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-blue-600">
              {areaPerRoom || '—'}
            </div>
            <div className="text-blue-700">m² per Room</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-blue-600">
              {((structural.suites || 0) / (structural.totalRooms || 1) * 100).toFixed(0)}%
            </div>
            <div className="text-blue-700">Suite Ratio</div>
          </div>
        </div>
      </div>

      {/* Building Compliance Notes */}
      {buildingAge > 50 && (
        <div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <svg className="h-5 w-5 text-amber-400" viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
              </svg>
            </div>
            <div className="ml-3">
              <h3 className="text-sm font-medium text-amber-800">Heritage Building Considerations</h3>
              <div className="mt-2 text-sm text-amber-700">
                <p>This building is over 50 years old and may require additional compliance considerations for listed buildings or conservation areas.</p>
              </div>
            </div>
          </div>
        </div>
      )}

      {!isEditing && (
        <div className="text-center py-4">
          <p className="text-sm text-gray-500">
            Click "Edit Details" above to modify building information
          </p>
        </div>
      )}
    </div>
  );
}
