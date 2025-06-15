'use client';

import { UserPlus, Eye, Edit, Mail, Shield, Trash2, X, Plus, Check, Search, Filter, Users, Calendar, MapPin, Clock, AlertCircle, CheckCircle } from 'lucide-react';
import { useState, useEffect } from 'react';
import { User, UserCreate, UserUpdate } from '../types/user';
import { userService } from '../services/userService';
import { hotels } from '../lib/hotels';

interface InlineUserManagementProps {
  className?: string;
}

export default function InlineUserManagement({ className = '' }: InlineUserManagementProps) {
  // State management
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  // Modal states
  const [showAddModal, setShowAddModal] = useState(false);
  const [showEditModal, setShowEditModal] = useState(false);
  const [showViewModal, setShowViewModal] = useState(false);
  const [selectedUser, setSelectedUser] = useState<User | null>(null);

  // Filter and search states
  const [searchTerm, setSearchTerm] = useState('');
  const [filterRole, setFilterRole] = useState('All Roles');
  const [filterHotel, setFilterHotel] = useState('All Hotels');
  const [filterStatus, setFilterStatus] = useState('All Status');
  const [showFilters, setShowFilters] = useState(false);

  // Form states
  const [newUser, setNewUser] = useState<UserCreate>({
    name: '',
    email: '',
    role: '',
    hotel: '',
    password: ''
  });

  const [editUser, setEditUser] = useState<UserUpdate>({});

  // Multi-select hotel state for forms
  const [selectedHotelsForAdd, setSelectedHotelsForAdd] = useState<string[]>([]);
  const [selectedHotelsForEdit, setSelectedHotelsForEdit] = useState<string[]>([]);
  const [showHotelDropdownAdd, setShowHotelDropdownAdd] = useState(false);
  const [showHotelDropdownEdit, setShowHotelDropdownEdit] = useState(false);

  // Available roles
  const availableRoles = [
    'System Admin',
    'Group Operations Manager',
    'Regional Manager',
    'Hotel Manager',
    'Assistant Manager',
    'Department Head',
    'Team Lead',
    'Staff Member',
    'Maintenance Lead',
    'Contractor'
  ];

  // Fetch users
  const fetchUsers = async () => {
    try {
      setLoading(true);
      const filters = {
        role: filterRole !== 'All Roles' ? filterRole : undefined,
        hotel: filterHotel !== 'All Hotels' ? filterHotel : undefined,
        status: filterStatus !== 'All Status' ? filterStatus : undefined,
        search: searchTerm || undefined
      };
      const userData = await userService.getUsers(filters);
      setUsers(userData);
      setError('');
    } catch (error) {
      setError(`Failed to fetch users: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchUsers();
  }, [filterRole, filterHotel, filterStatus, searchTerm]);

  // Clear messages after timeout
  useEffect(() => {
    if (error) {
      const timer = setTimeout(() => setError(''), 5000);
      return () => clearTimeout(timer);
    }
  }, [error]);

  useEffect(() => {
    if (success) {
      const timer = setTimeout(() => setSuccess(''), 3000);
      return () => clearTimeout(timer);
    }
  }, [success]);

  // Handle form submissions
  const handleAddUser = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!newUser.name || !newUser.email || !newUser.role || !newUser.password) {
      setError('Please fill in all required fields');
      return;
    }

    // Password validation
    if (newUser.password.length < 8) {
      setError('Password must be at least 8 characters long');
      return;
    }

    const hasUppercase = /[A-Z]/.test(newUser.password);
    const hasLowercase = /[a-z]/.test(newUser.password);
    const hasNumbers = /\d/.test(newUser.password);
    const hasSpecialChar = /[!@#$%^&*(),.?":{}|<>]/.test(newUser.password);

    if (!hasUppercase || !hasLowercase || !hasNumbers || !hasSpecialChar) {
      setError('Password must contain at least one uppercase letter, one lowercase letter, one number, and one special character');
      return;
    }

    const hotelString = selectedHotelsForAdd.length > 0 ? selectedHotelsForAdd.join(', ') : 'All Hotels';

    try {
      await userService.createUser({
        ...newUser,
        hotel: hotelString
      });
      setSuccess('User created successfully');
      setShowAddModal(false);
      setNewUser({ name: '', email: '', role: '', hotel: '', password: '' });
      setSelectedHotelsForAdd([]);
      fetchUsers();
    } catch (error) {
      setError(`Failed to create user: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  const handleEditUser = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedUser) return;

    const hotelString = selectedHotelsForEdit.length > 0 ? selectedHotelsForEdit.join(', ') : selectedUser.hotel;

    try {
      await userService.updateUser(selectedUser.id, {
        ...editUser,
        hotel: hotelString
      });
      setSuccess('User updated successfully');
      setShowEditModal(false);
      setEditUser({});
      setSelectedUser(null);
      setSelectedHotelsForEdit([]);
      fetchUsers();
    } catch (error) {
      setError(`Failed to update user: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  // Handle user actions
  const handleViewUser = (user: User) => {
    setSelectedUser(user);
    setShowViewModal(true);
  };

  const handleEditUserClick = (user: User) => {
    setSelectedUser(user);
    setEditUser({
      name: user.name,
      email: user.email,
      role: user.role,
      status: user.status
    });
    
    // Parse hotels for editing
    if (user.hotel && user.hotel !== 'All Hotels') {
      const userHotels = user.hotel.split(', ');
      setSelectedHotelsForEdit(userHotels);
    } else {
      setSelectedHotelsForEdit([]);
    }
    
    setShowEditModal(true);
  };

  const handleDeleteUser = async (userId: string, userName: string) => {
    if (!confirm(`Are you sure you want to permanently delete ${userName}? This action cannot be undone and will remove all user data.`)) {
      return;
    }

    try {
      const response = await userService.deleteUser(userId);
      setSuccess(response.message || 'User deleted successfully');
      fetchUsers();
    } catch (error) {
      setError(`Failed to delete user: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  const handleResetPassword = async (userId: string) => {
    const newPassword = prompt('Enter new password (minimum 8 characters with uppercase, lowercase, number, and special character):');
    if (!newPassword || newPassword.length < 8) {
      setError('Password must be at least 8 characters long');
      return;
    }

    // Basic password validation
    const hasUppercase = /[A-Z]/.test(newPassword);
    const hasLowercase = /[a-z]/.test(newPassword);
    const hasNumbers = /\d/.test(newPassword);
    const hasSpecialChar = /[!@#$%^&*(),.?":{}|<>]/.test(newPassword);

    if (!hasUppercase || !hasLowercase || !hasNumbers || !hasSpecialChar) {
      setError('Password must contain at least one uppercase letter, one lowercase letter, one number, and one special character');
      return;
    }

    try {
      const response = await userService.resetPassword(userId, newPassword);
      setSuccess(response.message || 'Password reset successfully');
    } catch (error) {
      setError(`Failed to reset password: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  const handleActivateUser = async (userId: string) => {
    try {
      const response = await userService.activateUser(userId);
      setSuccess(response.message || 'User activated successfully');
      fetchUsers();
    } catch (error) {
      setError(`Failed to activate user: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  // Hotel selection handlers
  const handleHotelToggle = (hotelName: string, isForEdit: boolean = false) => {
    if (isForEdit) {
      setSelectedHotelsForEdit(prev => 
        prev.includes(hotelName) 
          ? prev.filter(h => h !== hotelName)
          : [...prev, hotelName]
      );
    } else {
      setSelectedHotelsForAdd(prev => 
        prev.includes(hotelName) 
          ? prev.filter(h => h !== hotelName)
          : [...prev, hotelName]
      );
    }
  };

  // Get unique values for filters
  const uniqueRoles = [...new Set(users.map(user => user.role))];
  const uniqueHotels = [...new Set(users.map(user => user.hotel))];

  // Filter users based on search and filters
  const filteredUsers = users.filter(user => {
    const matchesSearch = user.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         user.email.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesRole = filterRole === 'All Roles' || user.role === filterRole;
    const matchesHotel = filterHotel === 'All Hotels' || user.hotel === filterHotel;
    const matchesStatus = filterStatus === 'All Status' || user.status === filterStatus;
    
    return matchesSearch && matchesRole && matchesHotel && matchesStatus;
  });

  // Helper function to get user initials
  const getUserInitials = (name: string) => {
    return name.split(' ').map(n => n[0]).join('').toUpperCase().slice(0, 2);
  };

  // Helper function to format date
  const formatDate = (dateString: string | null) => {
    if (!dateString) return 'Never';
    return new Date(dateString).toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  // Helper function to get status badge color
  const getStatusBadgeColor = (status: string) => {
    return status === 'Active' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800';
  };

  return (
    <>
      <div className={`bg-white rounded-lg border ${className}`}>
        {/* Header Section */}
        <div className="px-6 py-4 border-b border-gray-200">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <div className="flex items-center space-x-3">
              <div className="p-2 bg-blue-100 rounded-lg">
                <Users className="w-5 h-5 text-blue-600" />
              </div>
              <div>
                <h2 className="text-lg font-semibold text-gray-900">User Management</h2>
                <p className="text-sm text-gray-600">{filteredUsers.length} of {users.length} users</p>
              </div>
            </div>
            
            <button
              onClick={() => setShowAddModal(true)}
              className="inline-flex items-center px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
            >
              <UserPlus className="w-4 h-4 mr-2" />
              Add User
            </button>
          </div>

          {/* Search and Filters */}
          <div className="mt-4 space-y-3">
            <div className="flex flex-col sm:flex-row gap-3">
              <div className="flex-1 relative">
                <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search users by name or email..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              
              <button
                onClick={() => setShowFilters(!showFilters)}
                className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 transition-colors"
              >
                <Filter className="w-4 h-4 mr-2" />
                Filters
              </button>
            </div>

            {showFilters && (
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 p-4 bg-gray-50 rounded-lg">
                <select
                  value={filterRole}
                  onChange={(e) => setFilterRole(e.target.value)}
                  className="px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="All Roles">All Roles</option>
                  {uniqueRoles.map(role => (
                    <option key={role} value={role}>{role}</option>
                  ))}
                </select>

                <select
                  value={filterHotel}
                  onChange={(e) => setFilterHotel(e.target.value)}
                  className="px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="All Hotels">All Hotels</option>
                  {uniqueHotels.map(hotel => (
                    <option key={hotel} value={hotel}>{hotel}</option>
                  ))}
                </select>

                <select
                  value={filterStatus}
                  onChange={(e) => setFilterStatus(e.target.value)}
                  className="px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="All Status">All Status</option>
                  <option value="Active">Active</option>
                  <option value="Inactive">Inactive</option>
                </select>
              </div>
            )}
          </div>
        </div>

        {/* Status Messages */}
        {error && (
          <div className="mx-6 mt-4 p-3 bg-red-50 border border-red-200 rounded-lg flex items-center">
            <AlertCircle className="w-5 h-5 text-red-500 mr-2 flex-shrink-0" />
            <span className="text-red-700 text-sm">{error}</span>
          </div>
        )}

        {success && (
          <div className="mx-6 mt-4 p-3 bg-green-50 border border-green-200 rounded-lg flex items-center">
            <CheckCircle className="w-5 h-5 text-green-500 mr-2 flex-shrink-0" />
            <span className="text-green-700 text-sm">{success}</span>
          </div>
        )}

        {/* Users Table */}
        <div className="overflow-x-auto">
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
              <span className="ml-3 text-gray-600">Loading users...</span>
            </div>
          ) : filteredUsers.length === 0 ? (
            <div className="text-center py-12">
              <Users className="w-12 h-12 text-gray-400 mx-auto mb-4" />
              <p className="text-gray-500">No users found matching your criteria</p>
            </div>
          ) : (
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">User</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Role</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Hotel Access</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Last Login</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {filteredUsers.map((user) => (
                  <tr key={user.id} className="hover:bg-gray-50 transition-colors">
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center">
                        <div className="w-10 h-10 bg-gradient-to-br from-blue-500 to-purple-600 rounded-full flex items-center justify-center text-white font-medium text-sm">
                          {getUserInitials(user.name)}
                        </div>
                        <div className="ml-4">
                          <div className="text-sm font-medium text-gray-900">{user.name}</div>
                          <div className="text-sm text-gray-500">{user.email}</div>
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm text-gray-900">{user.role}</div>
                    </td>
                    <td className="px-6 py-4">
                      <div className="text-sm text-gray-900">
                        {user.hotel === 'All Hotels' ? (
                          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                            All Hotels
                          </span>
                        ) : (
                          <span className="text-sm text-gray-900">{user.hotel}</span>
                        )}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusBadgeColor(user.status)}`}>
                        {user.status}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm text-gray-900 flex items-center">
                        <Clock className="w-4 h-4 mr-1 text-gray-400" />
                        {formatDate(user.last_login)}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                      <div className="flex items-center space-x-2">
                        <button
                          onClick={() => handleViewUser(user)}
                          className="text-blue-600 hover:text-blue-900 p-1 rounded transition-colors"
                          title="View Details"
                        >
                          <Eye className="w-4 h-4" />
                        </button>
                        <button
                          onClick={() => handleEditUserClick(user)}
                          className="text-indigo-600 hover:text-indigo-900 p-1 rounded transition-colors"
                          title="Edit User"
                        >
                          <Edit className="w-4 h-4" />
                        </button>
                        <button
                          onClick={() => window.open(`mailto:${user.email}`, '_blank')}
                          className="text-green-600 hover:text-green-900 p-1 rounded transition-colors"
                          title="Send Email"
                        >
                          <Mail className="w-4 h-4" />
                        </button>
                        <button
                          onClick={() => handleResetPassword(user.id)}
                          className="text-orange-600 hover:text-orange-900 p-1 rounded transition-colors"
                          title="Reset Password"
                        >
                          <Shield className="w-4 h-4" />
                        </button>
      
